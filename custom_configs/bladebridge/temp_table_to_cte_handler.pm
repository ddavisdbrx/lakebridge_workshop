#!/usr/bin/perl

=head1 NAME

TempTableToCTEHandler - Convert T-SQL temp tables to Databricks CTEs

=head1 DESCRIPTION

BladeBridge preprocessing handler converting T-SQL procedures with temp tables 
to Databricks SQL with CTEs. Handles interleaved operations, optimized for large files.

=cut

package main;

use strict;
use warnings;

# BladeBridge global variables
our $MR;  # Message/logging handler provided by BladeBridge framework

# Preprocess: Convert SELECT INTO permanent tables to INSERT INTO
# This prevents base config from mangling them into SET statements
# CRITICAL: Do this BEFORE calling parent to prevent parent from seeing SELECT INTO
sub preprocess_select_into_table {
    my $array_cont = shift;
    my $extra_params = shift;

    # First, join and do OUR transformation BEFORE parent processes
    my $cont = join("\n", @$array_cont);

    # REMOVED: Only process stored procedures check
    # Process ALL SQL content, not just procedures
    
    # FIRST: Handle DROP TABLE IF EXISTS followed by SELECT INTO
    my $drop_count = 0;
    $cont =~ s{
        DROP\s+TABLE\s+IF\s+EXISTS\s+
        (?!\#)
        ((?:\[?[\w]+\]?\.)*\[?[\w]+\]?)
        \s*;\s*
        SELECT\s+
        (?:TOP\s+\d+\s+)?
        (?:DISTINCT\s+)?
        ((?:(?!INTO)[^;])+?)
        \s+INTO\s+
        (?!\#)
        \1
        \s+
        (FROM\s+[^;]+);
    }{
        $drop_count++;
        "CREATE OR REPLACE TABLE $1 AS\nSELECT $2\n$3;";
    }gixse;
    
    $MR->log_msg("Preprocessing: Converted $drop_count DROP TABLE IF EXISTS + SELECT INTO to CREATE OR REPLACE TABLE") if $drop_count > 0 && $MR;

    # SECOND: Handle standalone SELECT ... INTO PermanentTable
    my $insert_count = 0;
    $cont =~ s{
        \bSELECT\b\s+
        (?:TOP\s+\d+\s+)?
        (?:DISTINCT\s+)?
        ((?:(?!INTO)[^;])+?)
        \s+INTO\s+
        (?!\#)
        ((?:\[?[\w]+\]?\.)*\[?[\w]+\]?)
        \s+
        (FROM\s+[^;]+);
    }{
        $insert_count++;
        "INSERT INTO $2 BY NAME\nSELECT $1\n$3;";
    }gixse;
    
    $MR->log_msg("Preprocessing: Converted $insert_count SELECT INTO permanent table statement(s) to INSERT INTO") if $insert_count > 0 && $MR;

    # NOW split back to array and call parent preprocessing
    my @lines = split(/\n/, $cont);
    
    # Call parent preprocessing for standard transformations
    if (defined &handler_master_dbx_preprocess_file) {
        @lines = handler_master_dbx_preprocess_file(\@lines, $extra_params);
    }

    return @lines;
}

# Pre-finalization handler - converts temp table chains to CTEs
# This is called AFTER all fragments are converted and assembled
sub finalize_and_convert_temp_tables_to_cte
{
    my $array_cont = shift;  # Array ref of converted lines
    my $extra_params = shift;  # Optional hash ref with metadata
    
    # Call parent finalization first to preserve all processing
    # (restoring CASE WHEN, comments, string literals, etc.)
    my @finalized = finalize_content($array_cont, $extra_params);
    
    # Join the finalized content
    my $cont = join("\n", @finalized);
    
    # Only process if it contains a procedure with temp tables
    # At this stage: #TempTable → TEMP_TABLE_TempTable, CREATE TEMPORARY TABLE exists
    unless ($cont =~ /CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE/is && 
            $cont =~ /CREATE\s+TEMPORARY\s+TABLE\s+TEMP_TABLE_/is) {
        return;  # No temp tables to convert
    }
    
    $MR->log_msg("CTE Transformation: Converting temp table chain to CTEs") if $MR;
    
    # Extract procedure parts
    my ($header, $body, $trailing) = extract_procedure_parts($cont);
    
    # Parse the procedure body
    my ($temp_tables_ref, $other_operations_ref) = parse_procedure_body($body);
    
    # If no temp tables found after parsing, return
    unless (@$temp_tables_ref) {
        return;
    }
    
    $MR->log_msg("CTE Transformation: Processing " . scalar(@$temp_tables_ref) . " temp tables") if $MR;
    
    # Build hash of temp table name mappings
    my %temp_table_names;
    foreach my $block (@$temp_tables_ref) {
        $temp_table_names{'TEMP_TABLE_' . $block->{name}} = $block->{name};
    }
    
    # Build CTE chain and final operations
    my $cte_chain = build_cte_chain($temp_tables_ref, \%temp_table_names);
    my $final_operations = build_final_operations($other_operations_ref, \%temp_table_names);
    my $result = reconstruct_procedure($header, $cte_chain, $final_operations, $trailing);
    
    # CRITICAL: Pre-finalization handlers must MODIFY the input array in-place!
    # BladeBridge doesn't use the return value 
    @$array_cont = split(/\n/, $result);
    
    $MR->log_msg("CTE Transformation: Successfully converted to " . scalar(@$array_cont) . " lines with CTEs") if $MR;
    return;
}

# Extract procedure header, body, and trailing content
sub extract_procedure_parts
{
    my ($cont) = @_;
    
    # More flexible regex - just find BEGIN and END boundaries
    if ($cont =~ /^(.*?BEGIN\s*\n)(.*?)(END\s*;?\s*)$/is) {
        return ($1, $2, $3);
    }
    
    # If no BEGIN/END, treat entire content as body
    return ('', $cont, '');
}

# Parse procedure body into temp tables and other operations
sub parse_procedure_body
{
    my ($body) = @_;
    my @temp_tables;
    my @other_operations;
    my $remaining = $body;
    my $position = 0;
    
    while ($remaining) {
        
        # FIRST: Skip blank lines only (not comment blocks - those belong to statements)
        if ($remaining =~ /\G[ \t]*\n/gc) {
            $position = pos($remaining);
            next;  # Continue to next iteration
        }
        
        # Match temp table block (already converted syntax)
        if ($remaining =~ /\G((?:[ \t]*--[^\n]*\n)+)?[\s\n]*DROP\s+TEMPORARY\s+TABLE\s+IF\s+EXISTS\s+TEMP_TABLE_(\w+)\s*;[\s\n]+CREATE\s+TEMPORARY\s+TABLE\s+TEMP_TABLE_\2\s+AS[\s\n]+SELECT\s+(.*?)(FROM\s+.*?);/gics) {
            my ($comment, $table_name, $select_part, $from_part) = ($1, $2, $3, $4);
            $select_part =~ s/^\s+//; $select_part =~ s/\s+$//;
            $from_part =~ s/^\s+//; $from_part =~ s/\s+$//;
            push @temp_tables, {
                name => $table_name,
                comment => $comment || '',
                select_part => $select_part,
                from_part => $from_part,
                order => scalar(@temp_tables) + scalar(@other_operations)
            };
            $position = pos($remaining);
        }
        # Match INSERT INTO statements (now includes our converted permanent tables)
        elsif ($remaining =~ /\G((?:[ \t]*--[^\n]*\n)*?)[ \t]*(INSERT\s+INTO\s+[\w\.`\[\]]+\s+(?:AS\s+)?SELECT\s+[^;]+);/gics) {
            push @other_operations, {
                type => 'INSERT_INTO',
                comment => $1 || '',
                body => $2,
                order => scalar(@temp_tables) + scalar(@other_operations)
            };
            $position = pos($remaining);
        }

        elsif ($remaining =~ /\G((?:[ \t]*--[^\n]*\n)*?)[ \t]*([^;]+);/gcs) {
            my $full_statement = $2;
            # Trim leading/trailing whitespace
            $full_statement =~ s/^\s+//;
            $full_statement =~ s/\s+$//;
            
            push @other_operations, {
                type => 'OTHER',
                comment => $1 || '',
                body => $full_statement,
                order => scalar(@temp_tables) + scalar(@other_operations)
            };
            $position = pos($remaining);
        }
        # Advance one character if nothing matched (prevent infinite loop)
        else {
            last if length($remaining) == $position;
            $position++;
            pos($remaining) = $position;
        }
    }
    
    return (\@temp_tables, \@other_operations);
}

# Build final operations section from parsed non-temp operations
sub build_final_operations
{
    my ($other_operations_ref, $temp_table_names_ref) = @_;
    
    return '' unless @$other_operations_ref;
    
    # Build optimized regex pattern for all temp table replacements (O(n) optimization)
    my @sorted_keys = sort { length($b) <=> length($a) } keys %$temp_table_names_ref;
    my $temp_ref_pattern = @sorted_keys ? join('|', map { quotemeta($_) } @sorted_keys) : '';
    
    my @final_ops_parts;
    
    foreach my $op (@$other_operations_ref) {
        push @final_ops_parts, $op->{comment} if $op->{comment};
        
        if ($op->{type} eq 'INSERT_INTO') {
            # Remove TEMP_TABLE_ prefix from table references
            my $body = $op->{body};
            $body =~ s/($temp_ref_pattern)\b/$temp_table_names_ref->{$1}/ge if $temp_ref_pattern;
            push @final_ops_parts, "$body;\n\n";
        }
        else {
            # All other operations - remove TEMP_TABLE_ prefix
            my $body = $op->{body};
            $body =~ s/($temp_ref_pattern)\b/$temp_table_names_ref->{$1}/ge if $temp_ref_pattern;
            push @final_ops_parts, "$body;\n";
        }
    }
    
    my $final_ops = join('', @final_ops_parts);
    $final_ops =~ s/\n{3,}/\n\n/g;  # Clean up excessive blank lines
    
    return $final_ops;
}

# Build the CTE chain from temp table blocks
sub build_cte_chain
{
    my ($temp_tables_ref, $temp_table_names_ref) = @_;
    
    return '' unless @$temp_tables_ref;
    
    # Build optimized regex pattern for all temp table replacements (O(n) optimization)
    my @sorted_keys = sort { length($b) <=> length($a) } keys %$temp_table_names_ref;
    my $temp_ref_pattern = @sorted_keys ? join('|', map { quotemeta($_) } @sorted_keys) : '';
    
    my @cte_definitions;
    
    foreach my $block (@$temp_tables_ref) {
        my ($cte_name, $comment, $select_part, $from_part) = 
            ($block->{name}, $block->{comment}, $block->{select_part}, $block->{from_part});
        
        # Replace temp table references using optimized pattern
        $from_part =~ s/($temp_ref_pattern)\b/$temp_table_names_ref->{$1}/ge if $temp_ref_pattern;
        
        # Clean up comment
        $comment =~ s/^\s+//;
        $comment =~ s/\s+$//;
        
        # Build CTE definition (optimized array+join)
        push @cte_definitions, join('', 
            ($comment ? "\n$comment\n" : ''),
            "$cte_name AS (\n",
            "    SELECT \n        $select_part\n",
            "    $from_part\n)"
        );
    }
    
    return join(",\n", @cte_definitions);
}

# Reconstruct the complete procedure with CTE chain
sub reconstruct_procedure
{
    my ($header, $cte_chain, $final_ops, $trailing) = @_;
    
    # Build result - let standard config rules handle header transformations
    return join('', 
        $header,
        "\n-- Main CTE chain\n",
        "WITH $cte_chain\n",
        $final_ops,
        "\n$trailing"
    );
}

1;