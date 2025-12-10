import streamlit as st
import boto3
import json
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="Research Assistant",
    page_icon="🔬",
    layout="wide"
)

# Initialize session state
if 'messages' not in st.session_state:
    st.session_state.messages = []

def invoke_bedrock_agent(query, agent_id, agent_alias_id, session_id):
    print(query, agent_id, agent_alias_id, session_id)
    """
    Invoke the Bedrock agent with the user query
    """
    try:
        # Initialize Bedrock Agent Runtime client
        client = boto3.client(
            service_name='bedrock-agent-runtime',
            region_name=st.session_state.get('aws_region', 'us-east-2')
        )

        # Invoke the agent
        response = client.invoke_agent(
           agentId=agent_id,
            agentAliasId=agent_alias_id,
            sessionId=session_id,
            inputText=query
        )

        # Process the response stream
        output_text = ""
        for event in response.get('completion', []):
            if 'chunk' in event:
                chunk = event['chunk']
                if 'bytes' in chunk:
                    output_text += chunk['bytes'].decode('utf-8')

        return output_text, None

    except Exception as e:
        return None, str(e)

# Sidebar for configuration
with st.sidebar:
    st.header("⚙️ Configuration")

    # AWS Configuration
    st.subheader("AWS Settings")
    aws_region = st.text_input(
        "AWS Region",
        value="us-east-2",
        help="AWS region where your Bedrock agent is deployed"
    )
    st.session_state.aws_region = aws_region

    agent_id = st.text_input(
        "Agent ID",
        help="Your Bedrock agent ID",
        type="password"
    )

    agent_alias_id = st.text_input(
        "Agent Alias ID",
        help="Your Bedrock agent alias ID (e.g., TSTALIASID)",
        value="TSTALIASID"
    )

    session_id = st.text_input(
        "Session ID",
        value=f"session-{datetime.now().strftime('%Y%m%d%H%M%S')}",
        help="Unique session identifier"
    )

    st.divider()

    # Information section
    st.subheader("ℹ️ About")
    st.markdown("""
    This interface connects to your AWS Bedrock agent which:
    - 📄 Searches top 3 relevant papers
    - 📰 Finds latest news articles
    - 📝 Summarizes findings
    """)
    st.divider()

    # Clear chat button
    if st.button("🗑️ Clear Chat History", use_container_width=True):
        st.session_state.messages = []
        st.rerun()

# Main content area
st.title("🔬 Research Assistant")
st.markdown("Ask questions and get insights from academic papers and latest news")

# Display chat messages
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Chat input
if prompt := st.chat_input("What would you like to research?"):
    # Validate configuration
    if not agent_id:
        st.error("⚠️ Please enter your Agent ID in the sidebar")
        st.stop()

    if not agent_alias_id:
        st.error("⚠️ Please enter your Agent Alias ID in the sidebar")
        st.stop()

    # Add user message to chat
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Get agent response
    with st.chat_message("assistant"):
        with st.spinner("🔍 Searching papers and news..."):
            response_text, error = invoke_bedrock_agent(
                prompt,
                agent_id,
                agent_alias_id,
                session_id
            )

        if error:
            error_message = f"❌ **Error:** {error}\n\n*Please check your AWS credentials and agent configuration.*"
            st.error(error_message)
            st.session_state.messages.append({
                "role": "assistant",
                "content": error_message
            })
        elif response_text:
            st.markdown(response_text)
            st.session_state.messages.append({
                "role": "assistant",
                "content": response_text
            })
        else:
            no_response = "No response received from the agent."
            st.warning(no_response)
            st.session_state.messages.append({
                "role": "assistant",
                "content": no_response
            })

# Footer
st.divider()
st.caption("Powered by AWS Bedrock Agent | Built with Streamlit")
