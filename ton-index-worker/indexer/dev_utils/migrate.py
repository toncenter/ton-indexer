#!/usr/bin/env python3
"""
Migration tool for transferring and reclassifying traces with multiprocessing support.
"""

import argparse
import asyncio
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Dict, Any

from dev_utils.debug_api import list_tests, transfer_trace, reclassify_trace, TraceRequest


def worker_transfer_trace(trace_id: str) -> Dict[str, Any]:
    """Worker function to transfer a trace (runs in separate process)."""
    try:
        result = asyncio.run(transfer_trace(TraceRequest(trace_id=trace_id)))
        return {
            'trace_id': trace_id,
            'success': True,
            'result': result,
            'error': None
        }
    except Exception as e:
        return {
            'trace_id': trace_id,
            'success': False,
            'result': None,
            'error': str(e)
        }


def worker_reclassify_trace(trace_id: str) -> Dict[str, Any]:
    """Worker function to reclassify a trace (runs in separate process)."""
    try:
        result = asyncio.run(reclassify_trace(TraceRequest(trace_id=trace_id)))
        return {
            'trace_id': trace_id,
            'success': True,
            'result': result,
            'error': None
        }
    except Exception as e:
        return {
            'trace_id': trace_id,
            'success': False,
            'result': None,
            'error': str(e)
        }


def print_progress(completed: int, total: int, start_time: float):
    """Print progress information."""
    elapsed = time.time() - start_time
    rate = completed / elapsed if elapsed > 0 else 0
    eta = (total - completed) / rate if rate > 0 else 0
    
    print(f"\r[{completed}/{total}] {completed/total*100:.1f}% complete | "
          f"Rate: {rate:.1f} traces/s | "
          f"Elapsed: {elapsed:.1f}s | "
          f"ETA: {eta:.1f}s", end="", flush=True)


def process_traces(trace_ids: List[str], operation: str, max_workers: int = 8) -> Dict[str, Any]:
    """Process traces with multiprocessing."""
    total_traces = len(trace_ids)
    if total_traces == 0:
        print("No traces to process.")
        return {'success': 0, 'failed': 0, 'errors': []}
    
    print(f"\n🚀 Starting {operation} operation for {total_traces} traces using {max_workers} workers...")
    
    # Choose worker function based on operation
    worker_func = worker_transfer_trace if operation == 'transfer' else worker_reclassify_trace
    
    start_time = time.time()
    completed = 0
    success_count = 0
    error_count = 0
    errors = []
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_trace = {executor.submit(worker_func, trace_id): trace_id 
                          for trace_id in trace_ids}
        
        # Process completed tasks
        for future in as_completed(future_to_trace):
            result = future.result()
            completed += 1
            
            if result['success']:
                success_count += 1
            else:
                error_count += 1
                errors.append(result)
                print(f"\n❌ Error processing trace {result['trace_id']}: {result['error']}")
            
            # Update progress
            print_progress(completed, total_traces, start_time)
    
    # Final summary
    elapsed = time.time() - start_time
    print(f"\n\n✅ Migration completed!")
    print(f"📊 Summary:")
    print(f"   Total traces: {total_traces}")
    print(f"   Successful: {success_count}")
    print(f"   Failed: {error_count}")
    print(f"   Total time: {elapsed:.1f}s")
    print(f"   Average rate: {total_traces/elapsed:.1f} traces/s")
    
    return {
        'success': success_count,
        'failed': error_count,
        'errors': errors
    }


async def main():
    """Main function to coordinate the migration process."""
    parser = argparse.ArgumentParser(description='Migration tool for traces')
    parser.add_argument('--operation', '-o', 
                       choices=['transfer', 'reclassify'], 
                       required=True,
                       help='Operation to perform: transfer or reclassify traces')
    parser.add_argument('--workers', '-w', 
                       type=int, 
                       default=2,
                       help='Number of worker processes (default: 8)')
    parser.add_argument('--filter', '-f', 
                       type=str, 
                       default='',
                       help='Filter traces by name containing this string')
    parser.add_argument('--list-only', '-l', 
                       action='store_true',
                       help='Only list matching traces without processing')
    parser.add_argument('--max-traces', '-m', 
                       type=int, 
                       default=None,
                       help='Maximum number of traces to process')
    
    args = parser.parse_args()
    
    try:
        print("🔍 Fetching test data...")
        tests = await list_tests()
        print(f"📋 Found {len(tests)} total tests")
        
        # Apply filter if specified
        if args.filter:
            filtered_tests = [x for x in tests if args.filter.lower() in x['name'].lower()]
            print(f"🔍 Filtered to {len(filtered_tests)} tests containing '{args.filter}'")
        else:
            filtered_tests = tests
        
        # Extract trace IDs
        trace_ids = [x['trace_id'] for x in filtered_tests]

        # trace_ids = ["u20A2SBbl83yCl9l8IThi8zQlBCKTjl72PLEB806o2M="]
        # Apply max traces limit
        if args.max_traces and len(trace_ids) > args.max_traces:
            trace_ids = trace_ids[:args.max_traces]
            print(f"📏 Limited to first {args.max_traces} traces")
        
        # List matching traces
        if args.list_only:
            print("\n📝 Matching traces:")
            for i, test in enumerate(filtered_tests[:len(trace_ids)]):
                print(f"  {i+1:3d}. {test['name']} (ID: {test['trace_id']})")
            return
        
        # Process traces
        if trace_ids:
            result = process_traces(trace_ids, args.operation, args.workers)
            
            # Exit with error code if any traces failed
            if result['failed'] > 0:
                print(f"\n⚠️  {result['failed']} traces failed to process")
                sys.exit(1)
        else:
            print("ℹ️  No traces match the specified criteria")
            
    except KeyboardInterrupt:
        print("\n\n⏹️  Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())
