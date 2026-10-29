"""
CLI Entry point for benchmark execution.
Delegates to tests.benchmark_ext_authz.
"""

import asyncio
from tests.benchmark_ext_authz import main

if __name__ == "__main__":
    asyncio.run(main())
