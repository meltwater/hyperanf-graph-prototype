package it.unimi.dsi.big.webgraph.algo;

/*		 
 * Copyright (C) 2007-2015 Sebastiano Vigna 
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */


import com.martiansoftware.jsap.*;
import it.unimi.dsi.Util;
import it.unimi.dsi.big.webgraph.GraphClassParser;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.Transform.LabelledArcFilter;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator.LabelledArcIterator;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.AbstractLongComparator;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.logging.ProgressLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/** Computes the strongly connected components (and optionally the buckets) of an immutable graph.
 * 
 * <p>This class is a double implementation for debugging purposes.
 * 
 * <p>The {@link #compute(ImmutableGraph, boolean, ProgressLogger)} method of this class will return
 * an instance that contains the data computed by running a variant of Tarjan's algorithm on an immutable graph.
 * Besides the usually strongly connected components, it is possible to compute the <em>buckets</em> of the
 * graph, that is, nodes belonging to components that are terminal, but not dangling, in the component DAG.
 * 
 * <p>After getting an instance, it is possible to run the {@link #computeSizes()}
 * methods to obtain further information. This scheme has been devised to exploit the available memory as much
 * as possible&mdash;after the components have been computed, the returned instance keeps no track of
 * the graph, and the related memory can be freed by the garbage collector.
 * 
 * <h2>Stack size</h2>
 * 
 * <p>The method {@link #compute(ImmutableGraph, boolean, ProgressLogger)} might require a large stack size,
 * that should be set using suitable JVM options. Note, however,
 * that the stack size must be enlarged also on the operating-system side&mdash;for instance, using <code>ulimit -s unlimited</code>.
 */


public class StronglyConnectedComponentsTarjan {
	private static final Logger LOGGER = LoggerFactory.getLogger( StronglyConnectedComponentsTarjan.class );
	/** The number of strongly connected components. */
	final public long numberOfComponents;
	/** The component of each node. */
	final public long[][] component;
	/** The bit vector for buckets, or <code>null</code>, in which case buckets have not been computed. */
	final public LongArrayBitVector buckets;

	protected StronglyConnectedComponentsTarjan( final long numberOfComponents, final long[][] component, final LongArrayBitVector buckets ) {
		this.numberOfComponents = numberOfComponents;
		this.component = component;
		this.buckets = buckets;
	}  
	
	private final static class Visit {
		/** The graph. */
		private final ImmutableGraph graph;
		/** The number of nodes in {@link #graph}. */
		private final long n;
		/** A progress logger. */
		private final ProgressLogger pl;
		/** Whether we should compute buckets. */
		private final boolean computeBuckets;
		/** For non visited nodes, 0. For visited non emitted nodes the visit time. For emitted node -c-1, where c is the component number. */
		private final long[][] status;
		/** The buckets. */
		private final LongArrayBitVector buckets;
		/** The component stack. */
		private final LongBigArrayBigList stack;

		/** The first-visit clock (incremented at each visited node). */
		private long clock;
		/** The number of components already output. */
		private long numberOfComponents;

		private Visit( final ImmutableGraph graph, final long[][] status, final LongArrayBitVector buckets, ProgressLogger pl ) {
			this.graph = graph;
			this.buckets = buckets;
			this.status = status;
			this.pl = pl;
			this.computeBuckets = buckets != null;
			this.n = graph.numNodes();
			stack = new LongBigArrayBigList( n );
		}
		
		/** Visits a node.
		 * 
		 * @param x the node to visit.
		 * @return true if <code>x</code> is a bucket.
		 */
		private boolean visit( final long x ) {
			final long[][] status = this.status;
			if ( pl != null ) pl.lightUpdate();
			long statusX;
			LongBigArrays.set( status, x, statusX = ++clock );
			stack.add( x );

			long d = graph.outdegree( x );
			boolean noOlderNodeFound = true, isBucket = d != 0; // If we're dangling we're certainly not a bucket.
			
			if ( d != 0 ) {
				final LazyLongIterator successors = graph.successors( x );
				while( d-- != 0 ) {
					final long s = successors.nextLong();
					// If we can reach a non-bucket or another component we are not a bucket.
					if ( LongBigArrays.get( status, s ) == 0 && ! visit( s ) || LongBigArrays.get( status, s ) < 0 ) isBucket = false;
					final long statusS = LongBigArrays.get( status, s ); // Might have changed during the visit.
					if ( statusS > 0 && statusS < statusX ) {
						LongBigArrays.set( status, x, statusX = statusS );
						noOlderNodeFound = false;
					}
				}
			}

			if ( noOlderNodeFound ) {
				numberOfComponents++;
				long z;
				do {
					z = stack.removeLong( stack.size64() - 1 );
					// Component markers are -c-1, where c is the component number.
					LongBigArrays.set( status, z, -numberOfComponents );
					if ( isBucket && computeBuckets ) buckets.set( z, true );
				} while( z != x );
			}

			return isBucket;
		}
		
		
		public void run() {
			if ( pl != null ) {
				pl.itemsName = "nodes";
				pl.expectedUpdates = n;
				pl.displayFreeMemory = true;
				pl.start( "Computing strongly connected components..." );
			}
			for ( long x = 0; x < n; x++ ) if ( LongBigArrays.get( status, x ) == 0 ) visit( x );
			if ( pl != null ) pl.done();

			// Turn component markers into component numbers.
			for( int i = status.length; i-- != 0; ) {
				final long[] t = status[ i ];  
				for( int d = t.length; d-- != 0; ) t[ d ] = -t[ d ] - 1;
			}
				 			
			stack.add( numberOfComponents ); // Horrible kluge to return the number of components.
		}
	}
	
	/** Computes the strongly connected components of a given graph.
	 * 
	 * @param graph the graph whose strongly connected components are to be computed.
	 * @param computeBuckets if true, buckets will be computed.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return an instance of this class containing the computed components.
	 */
	public static StronglyConnectedComponentsTarjan compute( final ImmutableGraph graph, final boolean computeBuckets, final ProgressLogger pl ) {
		final long n = graph.numNodes();
		final Visit visit = new Visit( graph, LongBigArrays.newBigArray( n ), computeBuckets ? LongArrayBitVector.ofLength( n ) : null, pl );
		visit.run();
		return new StronglyConnectedComponentsTarjan( visit.numberOfComponents, visit.status, visit.buckets );
	}


	private final static class FilteredVisit {
		/** The graph. */
		private final ArcLabelledImmutableGraph graph;
		/** The number of nodes in {@link #graph}. */
		private final long n;
		/** A progress logger. */
		private final ProgressLogger pl;
		/** A filter on arc labels. */
		private final LabelledArcFilter filter;
		/** Whether we should compute buckets. */
		private final boolean computeBuckets;
		/** For non visited nodes, 0. For visited non emitted nodes the visit time. For emitted node -c-1, where c is the component number. */
		private final long[][] status;
		/** The buckets. */
		private final LongArrayBitVector buckets;
		/** The component stack. */
		private final LongBigArrayBigList stack;

		
		/** The first-visit clock (incremented at each visited node). */
		private long clock;
		/** The number of components already output. */
		private long numberOfComponents;
		
		private FilteredVisit( final ArcLabelledImmutableGraph graph, final LabelledArcFilter filter, final long[][] status, final LongArrayBitVector buckets, ProgressLogger pl ) {
			this.graph = graph;
			this.filter = filter;
			this.buckets = buckets;
			this.status = status;
			this.pl = pl;
			this.computeBuckets = buckets != null;
			this.n = graph.numNodes();
			stack = new LongBigArrayBigList( n );
		}
		
		/** Visits a node.
		 * 
		 * @param x the node to visit.
		 * @return true if <code>x</code> is a bucket.
		 */
		private boolean visit( final long x ) {
			final long[][] status = this.status;
			if ( pl != null ) pl.lightUpdate();
			LongBigArrays.set( status, x, ++clock );
			stack.add( x );
			long statusX = LongBigArrays.get( status, x );

			long d = graph.outdegree( x );
			long filteredDegree = 0;
			boolean noOlderNodeFound = true, isBucket = true;
			
			if ( d != 0 ) {
				final LabelledArcIterator successors = graph.successors( x );
				while( d-- != 0 ) {
					final long s = successors.nextLong();
					final long statusS = LongBigArrays.get( status, s );
					if ( ! filter.accept( x, s, successors.label() ) ) continue;
					filteredDegree++;
					// If we can reach a non-bucket or another component we are not a bucket.
					if ( statusS == 0 && ! visit( s ) || statusS < 0 ) isBucket = false;
					if ( statusS > 0 && statusS < statusX ) {
						LongBigArrays.set( status, x, statusX = statusS );
						noOlderNodeFound = false;
					}
				}
			}

			if ( filteredDegree == 0 ) isBucket = false;
			
			if ( noOlderNodeFound ) {
				numberOfComponents++;
				long z;
				do {
					z = stack.removeLong( stack.size64() - 1 );
					// Component markers are -c-1, where c is the component number.
					LongBigArrays.set( status, z, -numberOfComponents );
					if ( isBucket && computeBuckets ) buckets.set( z );
				} while( z != x );
			}
			
			return isBucket;
		}
		
		
		public void run() {
			if ( pl != null ) {
				pl.itemsName = "nodes";
				pl.expectedUpdates = n;
				pl.displayFreeMemory = true;
				pl.start( "Computing strongly connected components..." );
			}
			for ( long x = 0; x < n; x++ ) if ( LongBigArrays.get( status, x ) == 0 ) visit( x );
			if ( pl != null ) pl.done();

			// Turn component markers into component numbers.
			for( int i = status.length; i-- != 0; ) {
				final long[] t = status[ i ];  
				for( int d = t.length; d-- != 0; ) t[ d ] = -t[ d ] - 1;
			}
			
			stack.add( numberOfComponents ); // Horrible kluge to return the number of components.
		}
	}

	/** Computes the strongly connected components of a given arc-labelled graph, filtering its arcs.
	 * 
	 * @param graph the arc-labelled graph whose strongly connected components are to be computed.
	 * @param filter a filter selecting the arcs that must be taken into consideration.
	 * @param computeBuckets if true, buckets will be computed.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return an instance of this class containing the computed components.
	 */
	public static StronglyConnectedComponentsTarjan compute( final ArcLabelledImmutableGraph graph, final LabelledArcFilter filter, final boolean computeBuckets, final ProgressLogger pl ) {
		final long n = graph.numNodes();
		FilteredVisit filteredVisit = new FilteredVisit( graph, filter, LongBigArrays.newBigArray( n ), computeBuckets ? LongArrayBitVector.ofLength( n ) : null, pl );
		filteredVisit.run();
		return new StronglyConnectedComponentsTarjan( filteredVisit.numberOfComponents, filteredVisit.status, filteredVisit.buckets );
	}


	/** Returns the size big array for this set of strongly connected components.
	 * 
	 * @return the size big array for this set of strongly connected components.
	 */
	public long[][] computeSizes() {
		final long[][] size = LongBigArrays.newBigArray( numberOfComponents );
		for( int i = component.length; i-- != 0; ) {
			final long[] t = component[ i ];  
			for( int d = t.length; d-- != 0; ) LongBigArrays.incr( size, t[ d ] );
		}
		return size;
	}
	
	/** Renumbers by decreasing size the components of this set.
	 *
	 * <p>After a call to this method, both the internal status of this class and the argument
	 * array are permuted so that the sizes of strongly connected components are decreasing
	 * in the component index.
	 *
	 *  @param size the components sizes, as returned by {@link #computeSizes()}.
	 */
	public void sortBySize( final long[][] size ) {
		final long[][] perm = Util.identity( LongBigArrays.length( size ) );
		LongBigArrays.quickSort( perm, 0, LongBigArrays.length( perm ), new AbstractLongComparator() {
			public int compare( final long x, final long y ) {
				final long t = LongBigArrays.get( size, y ) - LongBigArrays.get( size, x );
				return t == 0 ? 0 : t < 0 ? -1 : 1;
			}
		});
		final long[][] copy = LongBigArrays.copy( size );

		for( int i = size.length; i-- != 0; ) {
			final long[] t = size[ i ];  
			final long[] u = perm[ i ];  
			for( int d = t.length; d-- != 0; ) t[ d ] = LongBigArrays.get( copy, u[ d ] );
		}
		Util.invertPermutationInPlace( perm );
		
		for( int i = component.length; i-- != 0; ) {
			final long[] t = component[ i ];  
			for( int d = t.length; d-- != 0; ) t[ d ] = LongBigArrays.get( perm, t[ d ] );
		}
	}

	public static void main( String arg[] ) throws IOException, JSAPException {
		SimpleJSAP jsap = new SimpleJSAP( StronglyConnectedComponentsTarjan.class.getName(), 
				"Computes the strongly connected components (and optionally the buckets) of a graph of given basename. The resulting data is saved " +
				"in files stemmed from the given basename with extension .scc (a list of binary integers specifying the " +
				"component of each node), .sccsizes (a list of binary integer specifying the size of each component) and .buckets " +
				" (a serialised BitSet specifying buckets). Please use suitable JVM options to set a large stack size.",
				new Parameter[] {
			new Switch( "sizes", 's', "sizes", "Compute component sizes." ),
			new Switch( "renumber", 'r', "renumber", "Renumber components in decreasing-size order." ),
			new Switch( "buckets", 'b', "buckets", "Compute buckets (nodes belonging to a bucket component, i.e., a terminal nondangling component)." ),
			new FlaggedOption( "filter", new ObjectParser( LabelledArcFilter.class, GraphClassParser.PACKAGE ), JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'f', "filter", "A filter for labelled arcs; requires the provided graph to be arc labelled." ),
			new FlaggedOption( "logInterval", JSAP.LONG_PARSER, Long.toString( ProgressLogger.DEFAULT_LOG_INTERVAL ), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds." ),
			new UnflaggedOption( "basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph." ),
			new UnflaggedOption( "resultsBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, JSAP.NOT_GREEDY, "The basename of the resulting files." ),
		}		
		);

		JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) System.exit( 1 );

		final String basename = jsapResult.getString( "basename" );
		final String resultsBasename = jsapResult.getString( "resultsBasename", basename );
		final LabelledArcFilter filter = (LabelledArcFilter)jsapResult.getObject( "filter" );
		ProgressLogger pl = new ProgressLogger( LOGGER, jsapResult.getLong( "logInterval" ), TimeUnit.MILLISECONDS );

		final StronglyConnectedComponentsTarjan components = 
			filter != null ? StronglyConnectedComponentsTarjan.compute( ArcLabelledImmutableGraph.load( basename ), filter, jsapResult.getBoolean( "buckets" ), pl )
					: StronglyConnectedComponentsTarjan.compute( ImmutableGraph.load( basename ), jsapResult.getBoolean( "buckets" ), pl );

		if ( jsapResult.getBoolean( "sizes" ) || jsapResult.getBoolean( "renumber" ) ) {
			final long[][] size = components.computeSizes();
			if ( jsapResult.getBoolean( "renumber" ) ) components.sortBySize( size );
			if ( jsapResult.getBoolean( "sizes" ) ) BinIO.storeLongs( size, resultsBasename + ".sccsizes" );
		}
		BinIO.storeLongs( components.component, resultsBasename + ".scc" );
		if ( components.buckets != null ) BinIO.storeObject( components.buckets, resultsBasename + ".buckets" );
	}
}
