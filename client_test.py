import asyncio
import os
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from langchain_mcp_adapters.tools import load_mcp_tools
from langgraph.prebuilt import create_react_agent
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage, AIMessage

# Thiết lập API Key cho Gemini
os.environ["GOOGLE_API_KEY"] = "AIzaSyC6J-jVfvl1YmTvigSMaYEGNLqgLGAwAUw"

async def main():
    # Khởi tạo mô hình Gemini
    model = ChatGoogleGenerativeAI(
        model="gemini-2.0-flash",
        google_api_key=os.getenv("GOOGLE_API_KEY")
    )

    # Thiết lập server MCP
    server_params = StdioServerParameters(
        command="python",
        args=["mcp_tool.py"],  # Đường dẫn đến server MCP
    )

    # Bộ nhớ lưu trữ tin nhắn
    message_history = []

    # Kết nối với server MCP
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await load_mcp_tools(session)

            # Tạo agent với Gemini và các công cụ từ server MCP
            agent = create_react_agent(model, tools)

            print("=== Chào mừng đến với Database Chat Agent ===")
            print("Nhập 'exit' hoặc 'quit' để thoát")
            
            # Khởi tạo với một câu lệnh mặc định
            initial_message = "Xin chào, tôi là Database Agent. Bạn có thể hỏi tôi về database hoặc yêu cầu tôi thực hiện các thao tác như crawl dữ liệu."
            message_history.append(AIMessage(content=initial_message))
            print("Agent: " + initial_message)

            # Vòng lặp chat
            while True:
                # Nhận input từ người dùng
                user_input = input("Bạn: ")
                
                # Kiểm tra nếu người dùng muốn thoát
                if user_input.lower() in ['exit', 'quit', 'thoát']:
                    print("Đang thoát khỏi chương trình...")
                    break
                
                # Thêm tin nhắn của người dùng vào lịch sử
                message_history.append(HumanMessage(content=user_input))
                
                try:
                    # Gửi toàn bộ lịch sử tin nhắn để duy trì ngữ cảnh
                    agent_response = await agent.ainvoke({"messages": message_history})
                    
                    # Lấy tin nhắn cuối cùng từ agent
                    ai_message = agent_response["messages"][-1]
                    
                    # Thêm tin nhắn của AI vào lịch sử
                    message_history.append(ai_message)
                    
                    # Hiển thị phản hồi
                    print("Agent:", ai_message.content)
                    
                except Exception as e:
                    error_message = f"Lỗi xảy ra: {str(e)}"
                    print("Agent (Error):", error_message)
                    # Thêm tin nhắn lỗi vào lịch sử
                    message_history.append(AIMessage(content=error_message))

# Chạy vòng lặp bất đồng bộ
if __name__ == "__main__":
    asyncio.run(main())