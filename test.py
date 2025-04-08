import asyncio
import os
from flask import Flask, request, jsonify, render_template
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from langchain_mcp_adapters.tools import load_mcp_tools
from langgraph.prebuilt import create_react_agent
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage, AIMessage

app = Flask(__name__)

# Thiết lập API Key cho Gemini
os.environ["GOOGLE_API_KEY"] = "AIzaSyC6J-jVfvl1YmTvigSMaYEGNLqgLGAwAUw"

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
async def setup_agent():
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await load_mcp_tools(session)
            agent = create_react_agent(model, tools)
            return agent

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
agent = loop.run_until_complete(setup_agent())

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/chat', methods=['POST'])
def chat():
    user_input = request.json.get('message', '')
    if not user_input:
        return jsonify({"error": "No message provided"}), 400
    
    # Thêm tin nhắn của người dùng vào lịch sử
    message_history.append(HumanMessage(content=user_input))
    
    try:
        # Gửi toàn bộ lịch sử tin nhắn để duy trì ngữ cảnh
        agent_response = loop.run_until_complete(agent.ainvoke({"messages": message_history}))
        
        # Lấy tin nhắn cuối cùng từ agent
        ai_message = agent_response["messages"][-1]
        
        # Thêm tin nhắn của AI vào lịch sử
        message_history.append(ai_message)
        
        # Trả về phản hồi
        return jsonify({"response": ai_message.content})
        
    except Exception as e:
        error_message = f"Lỗi xảy ra: {str(e)}"
        # Thêm tin nhắn lỗi vào lịch sử
        message_history.append(AIMessage(content=error_message))
        return jsonify({"error": error_message}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)