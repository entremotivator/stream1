import streamlit as st
import requests
import pandas as pd
import json
from datetime import datetime
import time

# Page configuration
st.set_page_config(
    page_title="WooWonder Data Extractor",
    page_icon="📊",
    layout="wide"
)

# Title and description
st.title("📊 WooWonder Data Extractor")
st.markdown("Export user data and articles from WooWonder to CSV files")

# Sidebar for configuration
st.sidebar.header("🔧 Configuration")

# API Configuration
with st.sidebar.expander("API Settings", expanded=True):
    site_url = st.text_input(
        "Site URL", 
        value="https://zzatem.com",
        help="Your WooWonder site URL"
    )
    
    access_token = st.text_input(
        "Access Token", 
        type="password",
        help="User's access token for authorization"
    )
    
    server_key = st.text_input(
        "Server Key", 
        value="ad18880474e60cd46a62b81194a6c296",
        type="password",
        help="Server key from Admin Panel"
    )

# Helper functions
def make_api_request(endpoint, post_data=None):
    """Make API request to WooWonder"""
    try:
        url = f"{site_url.rstrip('/')}/api/{endpoint}?access_token={access_token}"
        
        if post_data:
            post_data['server_key'] = server_key
            response = requests.post(url, data=post_data)
        else:
            response = requests.get(url)
        
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"API request failed: {str(e)}")
        return None
    except json.JSONDecodeError:
        st.error("Invalid JSON response from API")
        return None

def export_to_csv(data, filename):
    """Export data to CSV and provide download link"""
    if data:
        df = pd.DataFrame(data)
        csv = df.to_csv(index=False)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename_with_timestamp = f"{filename}_{timestamp}.csv"
        
        st.download_button(
            label=f"📥 Download {filename_with_timestamp}",
            data=csv,
            file_name=filename_with_timestamp,
            mime="text/csv"
        )
        
        return df
    return None

# Main content
if not all([site_url, access_token, server_key]):
    st.warning("⚠️ Please fill in all API configuration fields in the sidebar to proceed.")
else:
    # Create tabs for different data types
    tab1, tab2 = st.tabs(["👥 Users Data", "📰 Articles Data"])
    
    with tab1:
        st.header("👥 Users Data Extraction")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            user_ids_input = st.text_area(
                "User IDs (comma-separated)",
                placeholder="1,2,3,4,5",
                help="Enter user IDs separated by commas"
            )
        
        with col2:
            st.markdown("### Options")
            fetch_users_btn = st.button("🔄 Fetch Users Data", type="primary")
        
        if fetch_users_btn and user_ids_input:
            with st.spinner("Fetching users data..."):
                post_data = {
                    'user_ids': user_ids_input.strip()
                }
                
                result = make_api_request('get-many-users-data', post_data)
                
                if result and result.get('api_status') == 200:
                    users_data = result.get('users', [])
                    
                    if users_data:
                        st.success(f"✅ Successfully fetched {len(users_data)} users")
                        
                        # Display preview
                        st.subheader("📋 Data Preview")
                        df = pd.DataFrame(users_data)
                        st.dataframe(df, use_container_width=True)
                        
                        # Export option
                        st.subheader("💾 Export Data")
                        export_to_csv(users_data, "woowonder_users")
                        
                        # Show summary statistics
                        st.subheader("📊 Summary")
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Users", len(users_data))
                        with col2:
                            st.metric("Columns", len(df.columns))
                        with col3:
                            st.metric("Data Size", f"{df.memory_usage().sum()} bytes")
                    else:
                        st.warning("No users data found")
                else:
                    st.error("Failed to fetch users data. Please check your API configuration.")
    
    with tab2:
        st.header("📰 Articles Data Extraction")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.subheader("Filter Options")
            
            limit = st.number_input(
                "Limit (number of articles)",
                min_value=1,
                max_value=1000,
                value=25,
                help="Maximum number of articles to fetch"
            )
            
            offset = st.number_input(
                "Offset",
                min_value=0,
                value=0,
                help="Get articles after this offset ID"
            )
            
            user_id = st.number_input(
                "User ID (optional)",
                min_value=0,
                value=0,
                help="Filter articles by specific user ID (0 for all users)"
            )
            
            category_id = st.number_input(
                "Category ID (optional)",
                min_value=0,
                value=0,
                help="Filter articles by category ID (0 for all categories)"
            )
            
            article_id = st.number_input(
                "Specific Article ID (optional)",
                min_value=0,
                value=0,
                help="Get a specific article by ID (0 to ignore)"
            )
        
        with col2:
            st.markdown("### Actions")
            fetch_articles_btn = st.button("🔄 Fetch Articles", type="primary")
            
            st.markdown("### Quick Actions")
            if st.button("📊 Get Latest 100 Articles"):
                limit = 100
                fetch_articles_btn = True
        
        if fetch_articles_btn:
            with st.spinner("Fetching articles data..."):
                post_data = {
                    'limit': limit,
                    'offset': offset
                }
                
                if user_id > 0:
                    post_data['user_id'] = user_id
                if category_id > 0:
                    post_data['category'] = category_id
                if article_id > 0:
                    post_data['article_id'] = article_id
                
                result = make_api_request('get-articles', post_data)
                
                if result and result.get('api_status') == 200:
                    articles_data = result.get('articles', [])
                    
                    if articles_data:
                        st.success(f"✅ Successfully fetched {len(articles_data)} articles")
                        
                        # Display preview
                        st.subheader("📋 Data Preview")
                        df = pd.DataFrame(articles_data)
                        st.dataframe(df, use_container_width=True)
                        
                        # Export option
                        st.subheader("💾 Export Data")
                        export_to_csv(articles_data, "woowonder_articles")
                        
                        # Show summary statistics
                        st.subheader("📊 Summary")
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("Total Articles", len(articles_data))
                        with col2:
                            st.metric("Columns", len(df.columns))
                        with col3:
                            st.metric("Data Size", f"{df.memory_usage().sum()} bytes")
                        with col4:
                            if 'category' in df.columns:
                                st.metric("Unique Categories", df['category'].nunique())
                    else:
                        st.warning("No articles data found")
                else:
                    st.error("Failed to fetch articles data. Please check your API configuration.")

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666;'>
        <p>WooWonder Data Extractor | Built with Streamlit</p>
        <p><strong>Note:</strong> This tool is for authorized use only. Ensure you have proper permissions to access the API.</p>
    </div>
    """,
    unsafe_allow_html=True
)

# Instructions in sidebar
with st.sidebar.expander("📖 Instructions", expanded=False):
    st.markdown("""
    **How to use:**
    
    1. **Configure API Settings:**
       - Enter your WooWonder site URL
       - Provide a valid access token
       - Enter your server key
    
    2. **Users Data:**
       - Enter user IDs separated by commas
       - Click "Fetch Users Data"
       - Preview and download CSV
    
    3. **Articles Data:**
       - Set your filter options
       - Click "Fetch Articles"
       - Preview and download CSV
    
    **Tips:**
    - Use smaller limits for initial testing
    - Check your API rate limits
    - Verify your access token is valid
    """)

# Status indicators
if st.sidebar.button("🔍 Test API Connection"):
    with st.spinner("Testing API connection..."):
        test_result = make_api_request('get-articles', {'limit': 1})
        if test_result and test_result.get('api_status') == 200:
            st.sidebar.success("✅ API connection successful!")
        else:
            st.sidebar.error("❌ API connection failed. Check your settings.")
