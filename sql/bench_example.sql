####################################################################################
# BENCH plugin is basically a number generator. 
# it starts from 1. You specify the size of the loop size and the increment amount. 
# it will just iterate the loop over and over again. 
# if there are multiple number fields, the next field gets incremented each time the 
#    previous field completes one loop cycle.  
####################################################################################

# create a stream that generates numbers in a loop up to 1 billion

 CREATE STREAM bench_stream (
  num1  NUMBER
  ,num2  NUMBER
#   ,symbol string
 )
 USING BENCH(ceiling 1000000000 
             strings 'spy'
 );


####################################################################################
# creates a window with fixed size 1000. 
# go ahead and change it to 1 million. See what happens.
# or change the range to 1s to see how many millions it can pack in each second.
# Or remove some running aggregates to see it run even faster!

 CREATE WINDOW bench_last_1000 
 RUNNING avg, sum, slope, median, max, min , stddev
 FROM bench_stream.num1 
 # WHERE symbol like 'spy%' # you can have conditions for the window
 RANGE 1000;


####################################################################################
# this query selects fields num1 and num2 followed by aggregate functions. 
# It outputs to STDOUT. 
#The WHEN condition is always met. But the query sleeps for 1 second after each output. 
# After 20 outputs, the query automatically expires. 
# After the query drops, RioDB is still running. Kill the RioDB process before starting a new one. 

 SELECT num2, 
 num1, 'count', count,
 'AVG', avg, 'SUM', sum, 'SLOPE', slope, 'Med', median, 'max', max, 'min', min, 'stddev', stddev
 FROM bench_last_1000 
 WHEN bench_last_1000.avg > 0  # you can have conditions for the query.
#   and symbol in('spy','syp','pys') 
 OUTPUT stdout 
 LIMIT 20 
 TIMEOUT 1s;
 
############### FUTURE ENHANCEMENT
# Need ability to select directly from stream event, without window 
# select num1
# from bench_stream
# where num1 = 1;
 