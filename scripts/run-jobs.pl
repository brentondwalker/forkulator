#!/usr/bin/env perl

# Examples:
# ./scripts/run-jobs.pl s 1 2 1000000000 1 0.5
# ./scripts/run-jobs.pl s 2 4 1000000000 1 0.5
# ./scripts/run-jobs.pl s 1 2 1000000000 2 1
# ./scripts/run-jobs.pl s 2 4 1000000000 2 1
#
#

use strict;

if (@ARGV != 6) {
    print STDERR "usage: $0 <queue_type s|w> <num_workers> <num_tasks_per_job> <num_iterations> <service_facor> <arival_factor>\n";
    exit(0);
}
my ($q, $w, $t, $n, $service_f, $arrival_f) = @ARGV;

#my @service_rates = (1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1);
my @service_rates = (1.0);
my @arrival_rates = (0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0);
my $sample_interval = 1000;

for (my $i=0; $i<@service_rates; $i++) {
    $service_rates[$i] = $service_f * $service_rates[$i];
}

for (my $i=0; $i<@arrival_rates; $i++) {
    $arrival_rates[$i] = $arrival_f * $arrival_rates[$i];
}

foreach my $s (@service_rates) {
    foreach my $a (@arrival_rates) {
	#my $outfile = "data/erlang_".$q."q_w".$w."_t".$t."_a".$a."_s".$s."_n".$n."_sf".$service_f."_af".$arrival_f;
	#my $meansfile = "data/erlang_".$q."q_job_means_w".$w."_t".$t."_s".$s."_sf".$service_f."_af".$arrival_f.".dat";

	my $outfile = "lbfservice_data/".$q."q_w".$w."_t".$t."_a".$a."_s".$s."_n".$n."_sf".$service_f."_af".$arrival_f;
	my $meansfile = "lbfservice_data/".$q."q_job_means_w".$w."_t".$t."_s".$s."_sf".$service_f."_af".$arrival_f.".dat";

	my $cmd = "java -Xmx5g -cp bin forkulator.FJSimulator $q $w $t $a $s $n $sample_interval $outfile";
	print STDERR "$outfile \t $meansfile\n";
	print STDERR "$cmd\n";
	print `$cmd >> $meansfile`;
    }
}

