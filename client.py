import asyncio
import os
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from langchain_mcp_adapters.tools import load_mcp_tools
from langgraph.prebuilt import create_react_agent

from langchain_google_genai import ChatGoogleGenerativeAI  # Import Gemini

# Thiết lập API Key cho Gemini
os.environ["GOOGLE_API_KEY"] = "AIzaSyC6J-jVfvl1YmTvigSMaYEGNLqgLGAwAUw"

async def main():
    model = ChatGoogleGenerativeAI(model="gemini-2.0-flash" ,google_api_key=os.getenv("GOOGLE_API_KEY"))

    server_params = StdioServerParameters(
        command="python",
        args=["math_server.py"],  # Cập nhật đường dẫn nếu cần
    )

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await load_mcp_tools(session)

            # Tạo và chạy agent với Gemini
            agent = create_react_agent(model, tools)
            agent_response = await agent.ainvoke({"messages": "what's (3 + 5) x 12?"})

            print(agent_response["messages"][-1].content)

# Chạy vòng lặp bất đồng bộ
if __name__ == "__main__":
    asyncio.run(main())
