# Cloudera Matching - Solving the Assignment Problem on Hadoop using MapReduce and Giraph

## Introduction

The [assignment problem][ap] is a fundamental problem in combinatorial optimization that is concerned with
finding an optimal weighted matching in a bipartite graph. Quoting loosely from the [Wikipedia][ap] article:

> There are a number of agents and a number of tasks. Any agent can be assigned to perform any task, and provides
> some value that may vary depending on the agent-task assignment. It is required to perform all tasks by assigning
> exactly one agent to each task in such a way that the total value of the assignment is maximized.

  [ap]: http://en.wikipedia.org/wiki/Assignment_problem

This package uses a combination of [Crunch][cr] and [Giraph][gr] jobs running on CDH3 in order to provide a set of
tools for solving large-scale assignment problems. The implementation is based on [Bertsekas'][be] [auction algorithm][aa],
which is both fast and easy to parallelize on top of the graph-processing tools that Giraph provides.

  [cr]: http://github.com/cloudera/crunch
  [gr]: http://incubator.apache.org/giraph/
  [be]: http://web.mit.edu/dimitrib/www/home.html
  [aa]: http://18.7.29.232/bitstream/handle/1721.1/3154/P-1908-20783037.pdf?sequence=1

## Build and Installation

To use the matching tools, you will need to have a CDH3 cluster that has HDFS, MapReduce, and Zookeeper services
available. To build Giraph and the matching tools, run the following commands on a Linux-based system that has
git and Maven installed:

	git clone git://github.com/apache/giraph.git
	git clone git://github.com:cloudera/matching.git
	cd giraph
	mvn install
	cd ../matching
	mvn package

## Using the Matching Tools

To get started with the tools, you should create a text file that has three fields: the first is an alphanumeric
identifier for a vertex on one side of the bipartite graph (the "bidders"), the second is an alphanumeric
identifier for a vertex on the other side of the graph (the "objects") and the third value is a non-negative integer
that represents the weight that is assigned to the edge between two vertices. If there is no edge between two
vertices, there is no need to represent them in the file. The fields in each line of the file should be delimited
by a common character, like a comma or tab. For example, the file:

	a,x,3
	a,y,2
	b,x,1
	b,z,4
	c,x,0
	c,y,2
	c,z,3

represents a bipartite graph where the bidders are "a", "b", and "c", and the objects are "x", "y", and "z".

### Preparing the Input Data

In order to structure these edge oriented files into a format that Giraph can process, we run a MapReduce job written using
Crunch that takes our input file and constructs JSON records that represent the information we have available about each
vertex. In the scripts/ directory, the *prepare_input.sh* script demonstrates how to execute the Crunch job on the Hadoop
cluster after we have built the matchin tools.

	./scripts/prepare_input.sh /path/to/edges.txt /path/to/json_vertices ,

The script takes three arguments: the location of the input file on the cluster, the location to write the JSON-formatted
output records to, and the value of the delimiter character (in this case, a comma).

### Solving the Assignment Problem

Once the input has been prepared, we're ready to execute the Giraph job that solves the assignment problem. The input to
this job is the output from the input preparation step:

	./scripts/solve_assignment.sh /path/to/json_vertices /path/to/assignment_output <num_workers>

The <num_workers> value should be some integer value that is greater than zero and less than the total number of map slots
available on your Hadoop cluster. Giraph will claim those map slots in order to parallelize the job across the cluster.

### Processing the Output

The output of the Giraph job will be formatted JSON data that contains detailed information on the solution of the
assignment problem. If you are primarily interested in extracting the values of the vertices that were matched together,
we have another MapReduce job written using Crunch that will extract this data for you:

	./scripts/process_output.sh /path/to/assignment_output /path/to/matched_values_with_weights

