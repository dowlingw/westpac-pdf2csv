#!/usr/bin/env perl

#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.

use strict;
use warnings;

use DateTime;
use File::Temp qw/tempfile/;
use Readonly;
use Text::CSV;
use Pod::Usage;
use Getopt::Long;


#-----------------------------------------------------------------------------
# Parse commandline options

our( $opt_source, $opt_output, $opt_help, $opt_pdf2text ) = (undef, undef, undef, 'pdftotext');
GetOptions(
	'source=s',
	'output=s',
	'pdf2text=s',
	'help|?'
);

if(
	$opt_help ||
	!defined($opt_source) ||
	!defined($opt_output)
) {
	pod2usage(1)
}

unless( -d $opt_source && -r $opt_source ) {
	die "Source directory must be readable";
}

unless( -d $opt_output && -w $opt_output ) {
	die "Output directory must be writable";
}


#-----------------------------------------------------------------------------
# Magical lookup data

Readonly::Hash my %MONTH_IDX => ( 'JAN' => 1, 'FEB' => 2, 'MAR' => 3, 'APR' => 4, 'MAY' => 5, 'JUN' => 6, 'JUL' => 7, 'AUG' => 8, 'SEP' => 9, 'OCT' => 10, 'NOV' => 11, 'DEC' => 12 );

#-----------------------------------------------------------------------------
# Discover what accounts and statements exist

my $statements = {};

# Accounts
opendir(my $dh, $opt_source) || die "Can't open data dir";
while (readdir $dh) {
	next if( $_ =~ m/^\.+$/);
	$statements->{$_} = {};
}
closedir $dh;

# Statements
foreach my $acc ( keys %{$statements} ) {
	my $acc_dir = join( '/', $opt_source, $acc );
	next unless( -d $acc_dir );

	opendir(my $dh, $acc_dir) || die "Can't open statement directory";
	while (readdir $dh) {
		next unless ( $_ =~ m/^(\d+)\.pdf$/ );

		my $st_num = $1 * 1;
		my $st_file = join( '/', $acc_dir, $_ );

		$statements->{$acc}->{$st_num} = { file => $st_file };
	}
	closedir $dh;
}


#-----------------------------------------------------------------------------
# Parse all the statement PDFs

foreach my $acc ( sort keys %{$statements} ) {
	foreach my $st_num ( sort keys %{$statements->{$acc}} ) {
		my $st_file = $statements->{$acc}->{$st_num}->{'file'};

		# Slurp out the statement from PDF
		my ($fh, $fn) = tempfile("/tmp/XXXXXXX", UNLINK => 1);
		system( $opt_pdf2text, "-layout", $st_file, $fn ) == 0 || die "Failed to convert statement";
		my $data = parse_statement( $fh );
		close($fh);

		# Validation
		my @errors = validate_statement( $data );
		if( scalar @errors != 0 ) {
			print "Statement $st_num for $acc failed with errors:\n";
			print "\t".join( "\n\t", @errors )."\n";
			exit 1;
		}


		$statements->{$acc}->{$st_num}->{'data'} = $data;
	}
}

#-----------------------------------------------------------------------------
# Generate CSVs

my $csv = Text::CSV->new ({ binary => 1, always_quote => 1 });
foreach my $acc ( sort keys %{$statements} ) {
	foreach my $st_num ( sort keys %{$statements->{$acc}} ) {

		# Don't generate empty files
		next unless( scalar @{$statements->{$acc}->{$st_num}->{'data'}->{'txns'}} > 0 );

		my $csv_fn = join( '_', $acc, $st_num ).".csv";
		my $csv_path = join( '/', $opt_output, $csv_fn );

		open( my $fh, ">$csv_path" ) || die "Could not open CSV for writing: $csv_fn";

		foreach my $txn ( @{$statements->{$acc}->{$st_num}->{'data'}->{'txns'}} ) {
			$csv->print( $fh, [
				$acc,
				$st_num,
				$txn->{'date'}->strftime("%F"),
				$txn->{'amount'},
				$txn->{'description'}
			]);
			print $fh "\n";
		}

		close($fh);
	}
}


#-----------------------------------------------------------------------------
# Supporting methods

sub parse_statement {
	my ($fh) = @_;
	my $data = {
		txns => []
	};

	my $balance = undef;
	my $reading = 0;
	my $datecursor = undef;

	while( ! eof $fh ) {
		my $line = readline($fh);
		chomp $line;

		# OLD FORMAT: Get summary data to ensure we read all lines correctly
		if( $line =~ m/OPENING BALANCE\s*TOTAL CREDITS\s*TOTAL DEBITS\s*CLOSING BALANCE/ ) {
			if( readline($fh) =~ m/^\s*(.) \$([0-9\.]+)\s*\$([0-9\.]+)\s*\$([0-9\.]+)\s*(.) \$([0-9\.]+)\s*$/ ) {
				$data->{'open'} = $2 * ($1 eq '-' ? -1 : 1);
				$data->{'close'} = $6 * ($5 eq '-' ? -1 : 1);
				$data->{'credits'} = $3 * 1;
				$data->{'debits'} = $4 * 1;

				$balance = $data->{'open'};
				$reading = 1;
			}
		}

		# OLD FORMAT: Get reporting range
		if( $line =~ m/(?:FOR THE PERIOD FROM|FROM LAST STATEMENT DATED)\s*(\d+) (\w{3}) (\d+)\s*TO\s*(\d+) (\w{3}) (\d+)/i ) {
			$data->{'date_from'} = parsedate($1,uc $2,$3);
			$data->{'date_to'} = parsedate($4, uc$5,$6);

			$datecursor = $data->{'date_from'};
		}

		# NEW FORMAT: Get summary data
		if( $line =~ m/Opening Balance\s*([\+\-]?) \$([\d,]+\.\d{2})\s*$/ ) {
			my ($m1,$m2) = ($1,$2);

			$m2 =~ s/,//gi;
			$data->{'open'} = $m2 * ($m1 eq '-' ? -1 : 1);
			$balance = $data->{'open'};
		}
		if( $line =~ m/Total credits\s*[\+\-] \$([\d,]+\.\d{2})\s*$/ ) {
			my $m1 = $1; $m1 =~ s/,//gi;
			$data->{'credits'} = $m1 * 1;
		}
		if( $line =~ m/Total debits\s*[\+\-] \$([\d,]+\.\d{2})\s*$/ ) {
			my $m1 = $1; $m1 =~ s/,//gi;
			$data->{'debits'} = $m1 * 1;
		}

		# NEW FORMAT: Get reporting data
		if( $line =~ m/From Last Statement Dated\s*(\d{1,2}) (\w{3}) (\d{4})\s*to\s*(\d{1,2}) ([A-Z][a-z]{2}) (\d{4})/i ) {
			$data->{'date_from'} = parsedate($1,uc $2,$3);
			$data->{'date_to'} = parsedate($4,uc $5,$6);
		}

		# NEW FORMAT: Detect the start of statement lines
		if( $line =~ m/^\s*Date\s*Description of transaction\s*Debit\s*Credit\s*Balance\s*$/i ) {
			$reading = 1;
		}

		# BOTH FORMATS: Statement line
		if( $reading && $line =~ m/^\s*(\d{2})\s*(\w{3})\s*(.*)\s*$/ ) {
			my ($d,$month,$frag) = ($1,$2,$3);

			next unless( defined $frag );
			if( $frag =~ m/^CLOSING BALANCE/ ) {
				$reading = 0;
				last;
			}

			my ($m1,$m2, @descparts);
			while(1) {
				if( $frag =~ m/^\s*(.*?)\s*([0-9\-\,]+\.\d{2}+)\s*([0-9\-\,]+\.\d{2}+)$/ ) {
					($m1,$m2) = ($2,$3);
					my $f1 = $1;
					$m1 =~ s/,//g;
					$m2 =~ s/,//g;
					push( @descparts, trim($f1) );
					last;
				}

				push( @descparts, trim($frag) );
				$frag = readline($fh);
			}

			my $newbal = $m2 * 1;
			my $amount = $m1 * ($newbal > $balance ? 1 : -1);

			# What date is this transaction?
			# Hacky - we just find the next DDMMM combo following the previous date
			# This lets us handle date wrapping around new years and isn't correct in all situations.
			# A better solution would be to parse the year prefix in other lines - but this works for all the sample data I need
			my $resolved_date = undef;
			for( my $date = $datecursor; $date <= $data->{'date_to'}; $date->add( days => 1) ) {
				if( $date->day() == $d && uc $date->month_abbr() eq uc $month ) {
					$resolved_date = $date;
					last;
				}
			}

			my $txn = {
				date => $resolved_date,
				amount => $amount,
				description => join( "\n", @descparts )
			};

			push( @{$data->{'txns'}}, $txn );
			$balance = $newbal;
		}

	}

	return $data;
}

sub validate_statement {
	my ($data) = @_;
	my @errors;

	push( @errors, "DATE FROM not found" ) unless( defined $data->{'date_from'} );
	push( @errors, "DATE TO not found" ) unless( defined $data->{'date_to'} );

	push( @errors, "OPENING BALANCE not found" ) unless( defined $data->{'open'} );

	my ($deb,$cred) = (0.00, 0.00);
	my $no_date = 0;
	foreach my $txn ( @{$data->{'txns'}} ) {
		if( $txn->{'amount'} > 0 ) {
			$cred += $txn->{'amount'};
		} else {
			$deb += -1 * $txn->{'amount'};
		}
		if( ! defined $txn->{'date'} ) {
			$no_date++;
		}
	}

	if( defined $data->{'credits'} ) {
		push( @errors, "CREDITS: Expected=$data->{'credits'}, Actual=$cred" ) unless( $cred eq $data->{'credits'} );
	} else {
		push( @errors, "Summary CREDITS amount not found" );
	}

	if( defined $data->{'debits'} ) {
		push( @errors, "DEBITS: Expected=$data->{'debits'}, Actual=$deb" ) unless( $deb eq $data->{'debits'} );
	} else {
		push( @errors, "Summary DEBITS amount not found" );
	}

	if( $no_date > 0 ) {
		push( @errors, "Transaction date not found for $no_date transactions" );
	}

	return @errors;
}

sub parsedate {
	my ($d,$m,$y) = @_;
	
	return DateTime->new(
		year => $y,
		month => $MONTH_IDX{$m},
		day => $d
	);
}

sub trim {
	my ($str) = @_;

	$str =~ s/^\s*//g;
	$str =~ s/\s*$//g;

	return $str;
}


#-----------------------------------------------------------------------------
# POD Documentation


__END__

=head1 NAME

westpac-pdf2csv - Convert Westpac PDF statements to CSV

=head1 SYNOPSIS

westpac-pdf2csv.pl -source SRC_DIR -output DST_DIR [-pdf2text PDF2TEXT_BIN]

=head1 OPTIONS

=item B<-source>

Specify the source directory

=item B<-output>

Specify the output directory

=item B<-pdf2text>

Specify the full path to pdf2text binary

=item B<-help>

Print a brief help message and exits.
