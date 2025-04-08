from typing import List
from mcp.server.fastmcp import FastMCP
from langchain_community.utilities import SerpAPIWrapper

mcp = FastMCP("Weather")

@mcp.tool()
async def get_weather(location: str) -> str:
    """Get weather for location."""
    return "It's always sunny in New York"

if __name__ == "__main__":
    mcp.run(transport="sse")