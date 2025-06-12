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
st.markdown("Export user data and articles from WooWonder to CSV files with enhanced data processing")

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
def make_api_request(endpoint, post_data=None, retries=3):
    """Make API request to WooWonder with retry logic"""
    for attempt in range(retries):
        try:
            url = f"{site_url.rstrip('/')}/api/{endpoint}?access_token={access_token}"
            
            if post_data:
                post_data['server_key'] = server_key
                response = requests.post(url, data=post_data, timeout=30)
            else:
                response = requests.get(url, timeout=30)
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            if attempt == retries - 1:
                st.error(f"API request failed after {retries} attempts: {str(e)}")
                return None
            time.sleep(2 ** attempt)  # Exponential backoff
            
        except json.JSONDecodeError:
            st.error("Invalid JSON response from API")
            return None
    
    return None

def extract_author_info(author_data):
    """Extract author username and email from author field"""
    if not author_data:
        return None, None
    
    # If author_data is a string, try to parse it as JSON
    if isinstance(author_data, str):
        try:
            author_data = json.loads(author_data)
        except json.JSONDecodeError:
            return None, None
    
    # If it's a dictionary, extract username and email
    if isinstance(author_data, dict):
        username = author_data.get('username', '')
        email = author_data.get('email', '')
        return username, email
    
    return None, None

def bulk_fetch_articles(post_data, total_limit, progress_callback=None):
    """Fetch articles in bulk by paginating through API"""
    all_articles = []
    current_offset = post_data.get('offset', 0)
    api_limit = min(1000, post_data.get('limit', 1000))  # API max is 1000
    
    total_fetched = 0
    page = 1
    
    while total_fetched < total_limit:
        # Calculate how many to fetch in this request
        remaining = total_limit - total_fetched
        current_limit = min(api_limit, remaining)
        
        # Prepare request data
        current_post_data = post_data.copy()
        current_post_data['limit'] = current_limit
        current_post_data['offset'] = current_offset + total_fetched
        
        if progress_callback:
            progress_callback(f"Fetching page {page} (offset {current_post_data['offset']}, limit {current_limit})")
        
        # Make API request
        result = make_api_request('get-articles', current_post_data)
        
        if result and result.get('api_status') == 200:
            articles_batch = result.get('articles', [])
            
            if not articles_batch:
                # No more articles available
                break
            
            all_articles.extend(articles_batch)
            total_fetched += len(articles_batch)
            
            # If we got fewer articles than requested, we've reached the end
            if len(articles_batch) < current_limit:
                break
                
        else:
            st.error(f"Failed to fetch page {page}. Stopping bulk fetch.")
            break
        
        page += 1
        time.sleep(1)  # Rate limiting between requests
    
    return all_articles

def process_articles_data(articles_data):
    """Process articles data to extract author information and flatten nested fields"""
    processed_articles = []
    
    for article in articles_data:
        processed_article = article.copy()
        
        # Extract author information
        author_data = article.get('author', {})
        author_username, author_email = extract_author_info(author_data)
        
        processed_article['author_username'] = author_username
        processed_article['author_email'] = author_email
        
        # Flatten author data if it's a dict
        if isinstance(author_data, dict):
            for key, value in author_data.items():
                if key not in ['username', 'email']:  # Don't duplicate
                    processed_article[f'author_{key}'] = value
        
        # Process other nested fields
        if 'category' in article and isinstance(article['category'], dict):
            category_data = article['category']
            processed_article['category_name'] = category_data.get('name', '')
            processed_article['category_id'] = category_data.get('id', '')
        
        # Convert timestamps to readable format
        for date_field in ['time', 'created_at', 'updated_at']:
            if date_field in processed_article and processed_article[date_field]:
                try:
                    timestamp = int(processed_article[date_field])
                    processed_article[f'{date_field}_readable'] = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                except (ValueError, TypeError):
                    pass
        
        processed_articles.append(processed_article)
    
    return processed_articles

def process_users_data(users_data):
    """Process users data to flatten nested fields"""
    processed_users = []
    
    for user in users_data:
        processed_user = user.copy()
        
        # Flatten details field
        if 'details' in user and isinstance(user['details'], dict):
            details = user['details']
            for key, value in details.items():
                processed_user[f'details_{key}'] = value
        
        # Process notification settings
        if 'notification_settings' in user:
            try:
                if isinstance(user['notification_settings'], str):
                    notification_settings = json.loads(user['notification_settings'])
                else:
                    notification_settings = user['notification_settings']
                
                for key, value in notification_settings.items():
                    processed_user[f'notification_{key}'] = value
            except (json.JSONDecodeError, TypeError):
                pass
        
        # Convert timestamps to readable format
        for date_field in ['lastseen', 'last_data_update', 'point_day_expire']:
            if date_field in processed_user and processed_user[date_field]:
                try:
                    timestamp = int(processed_user[date_field])
                    processed_user[f'{date_field}_readable'] = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                except (ValueError, TypeError):
                    pass
        
        processed_users.append(processed_user)
    
    return processed_users

def export_to_csv(data, filename, process_func=None):
    """Export data to CSV with optional processing and provide download link"""
    if data:
        # Process data if processing function is provided
        if process_func:
            data = process_func(data)
        
        df = pd.DataFrame(data)
        
        # Clean column names
        df.columns = df.columns.str.replace('[^a-zA-Z0-9_]', '_', regex=True)
        
        csv = df.to_csv(index=False)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename_with_timestamp = f"{filename}_{timestamp}.csv"
        
        st.download_button(
            label=f"📥 Download {filename_with_timestamp}",
            data=csv,
            file_name=filename_with_timestamp,
            mime="text/csv",
            key=f"download_{filename}_{timestamp}"
        )
        
        return df
    return None

def display_data_preview(df, data_type="Data"):
    """Display enhanced data preview with filtering options"""
    st.subheader(f"📋 {data_type} Preview")
    
    # Search functionality
    search_term = st.text_input(f"🔍 Search {data_type.lower()}", key=f"search_{data_type}")
    
    if search_term:
        # Search across all string columns
        mask = df.astype(str).apply(lambda x: x.str.contains(search_term, case=False, na=False)).any(axis=1)
        filtered_df = df[mask]
        st.info(f"Showing {len(filtered_df)} results for '{search_term}'")
    else:
        filtered_df = df
    
    # Column selection with priority for author fields
    all_columns = list(df.columns)
    if len(all_columns) > 10:
        # Prioritize author fields and other important columns
        priority_columns = []
        
        # Always include author fields if available
        if 'author_username' in all_columns:
            priority_columns.append('author_username')
        if 'author_email' in all_columns:
            priority_columns.append('author_email')
        
        # Add other important columns
        important_columns = ['id', 'title', 'content', 'username', 'email', 'name', 'first_name', 'last_name']
        for col in important_columns:
            if col in all_columns and col not in priority_columns:
                priority_columns.append(col)
        
        # Fill remaining slots with other columns
        remaining_columns = [col for col in all_columns if col not in priority_columns]
        default_columns = priority_columns + remaining_columns[:10-len(priority_columns)]
        
        selected_columns = st.multiselect(
            f"Select columns to display (author fields selected by default)",
            options=all_columns,
            default=default_columns,
            key=f"columns_{data_type}"
        )
    else:
        selected_columns = all_columns
    
    # Display data
    if selected_columns:
        st.dataframe(filtered_df[selected_columns], use_container_width=True, height=400)
    else:
        st.warning("Please select at least one column to display")

# Main content
if not all([site_url, access_token, server_key]):
    st.warning("⚠️ Please fill in all API configuration fields in the sidebar to proceed.")
else:
    # Create tabs for different data types
    tab1, tab2, tab3 = st.tabs(["👥 Users Data", "📰 Articles Data", "📊 Analytics"])
    
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
            
            # Batch processing option
            batch_size = st.number_input("Batch Size", min_value=1, max_value=100, value=10, 
                                       help="Process users in batches to avoid API limits")
        
        if fetch_users_btn and user_ids_input:
            user_ids = [uid.strip() for uid in user_ids_input.split(',') if uid.strip()]
            
            if user_ids:
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                all_users_data = []
                
                # Process in batches
                for i in range(0, len(user_ids), batch_size):
                    batch_ids = user_ids[i:i+batch_size]
                    batch_ids_str = ','.join(batch_ids)
                    
                    status_text.text(f"Processing batch {i//batch_size + 1}/{(len(user_ids)-1)//batch_size + 1}")
                    
                    post_data = {'user_ids': batch_ids_str}
                    result = make_api_request('get-many-users-data', post_data)
                    
                    if result and result.get('api_status') == 200:
                        batch_users = result.get('users', [])
                        all_users_data.extend(batch_users)
                    
                    progress_bar.progress((i + batch_size) / len(user_ids))
                    time.sleep(0.5)  # Rate limiting
                
                if all_users_data:
                    st.success(f"✅ Successfully fetched {len(all_users_data)} users")
                    
                    # Process and display data
                    df = export_to_csv(all_users_data, "woowonder_users", process_users_data)
                    
                    if df is not None:
                        display_data_preview(df, "Users")
                        
                        # Show summary statistics
                        st.subheader("📊 Summary")
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("Total Users", len(all_users_data))
                        with col2:
                            st.metric("Columns", len(df.columns))
                        with col3:
                            st.metric("Active Users", len(df[df['active'] == '1']) if 'active' in df.columns else 0)
                        with col4:
                            st.metric("Verified Users", len(df[df['verified'] == '1']) if 'verified' in df.columns else 0)
                else:
                    st.warning("No users data found")
            else:
                st.error("Please enter valid user IDs")
    
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
            
            st.markdown("### Bulk Export")
            bulk_export_limit = st.selectbox(
                "Bulk Export Size",
                options=[1000, 2500, 5000, 10000],
                index=0,
                help="Automatically paginate through API to export large datasets"
            )
            
            bulk_export_btn = st.button("📦 Bulk Export", type="secondary")
            
            st.markdown("### Quick Actions")
            quick_100_btn = st.button("📊 Get Latest 100 Articles")
            quick_500_btn = st.button("📈 Get Latest 500 Articles")
        
        # Handle quick actions
        if quick_100_btn:
            limit = 100
            offset = 0
            fetch_articles_btn = True
        
        if quick_500_btn:
            limit = 500
            offset = 0
            fetch_articles_btn = True
        
        if fetch_articles_btn or bulk_export_btn:
            # Determine if this is a bulk export
            is_bulk_export = bulk_export_btn
            target_limit = bulk_export_limit if is_bulk_export else limit
            
            with st.spinner(f"Fetching {'bulk' if is_bulk_export else 'articles'} data..."):
                post_data = {
                    'limit': min(1000, target_limit),  # API limit per request
                    'offset': offset
                }
                
                if user_id > 0:
                    post_data['user_id'] = user_id
                if category_id > 0:
                    post_data['category'] = category_id
                if article_id > 0:
                    post_data['article_id'] = article_id
                
                # Initialize result and articles_data
                result = None
                articles_data = []
                
                if is_bulk_export and target_limit > 1000:
                    # Use bulk fetch for large requests
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    def progress_callback(message):
                        status_text.text(message)
                        # Update progress based on estimated completion
                        current_progress = min(len(articles_data) / target_limit if articles_data else 0, 1.0)
                        progress_bar.progress(current_progress)
                    
                    articles_data = bulk_fetch_articles(post_data, target_limit, progress_callback)
                    progress_bar.progress(1.0)
                    status_text.text("Bulk fetch completed!")
                    
                    # Create a mock result for consistency
                    result = {'api_status': 200} if articles_data else {'api_status': 404}
                    
                else:
                    # Single request for smaller datasets
                    result = make_api_request('get-articles', post_data)
                    articles_data = result.get('articles', []) if result and result.get('api_status') == 200 else []
                
                if result and result.get('api_status') == 200:
                    if articles_data:
                        st.success(f"✅ Successfully fetched {len(articles_data)} articles")
                        
                        # Process and display data
                        df = export_to_csv(articles_data, "woowonder_articles", process_articles_data)
                        
                        if df is not None:
                            display_data_preview(df, "Articles")
                            
                            # Show summary statistics
                            st.subheader("📊 Summary")
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric("Total Articles", len(articles_data))
                            with col2:
                                st.metric("Columns", len(df.columns))
                            with col3:
                                unique_authors = df['author_username'].nunique() if 'author_username' in df.columns else 0
                                st.metric("Unique Authors", unique_authors)
                            with col4:
                                unique_categories = df['category_name'].nunique() if 'category_name' in df.columns else 0
                                st.metric("Unique Categories", unique_categories)
                            
                            # Author statistics
                            if 'author_username' in df.columns:
                                st.subheader("👥 Author Statistics")
                                author_stats = df.groupby('author_username').size().sort_values(ascending=False)
                                st.bar_chart(author_stats.head(10))
                            
                            # Show fetch performance info
                            if is_bulk_export:
                                st.info(f"📈 Bulk export completed! Fetched {len(articles_data)} articles using automatic pagination.")
                    else:
                        st.warning("No articles data found")
                else:
                    st.error("Failed to fetch articles data. Please check your API configuration.")
    
    with tab3:
        st.header("📊 Analytics Dashboard")
        st.info("This section will show analytics once you fetch some data from the other tabs.")
        
        # Placeholder for analytics
        st.markdown("""
        **Available Analytics:**
        - User activity patterns
        - Article publishing trends
        - Category distribution
        - Author performance metrics
        - Engagement statistics
        
        *Fetch data from Users or Articles tabs to see analytics here.*
        """)

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666;'>
        <p>Enhanced WooWonder Data Extractor | Built with Streamlit</p>
        <p><strong>Note:</strong> This tool is for authorized use only. Ensure you have proper permissions to access the API.</p>
    </div>
    """,
    unsafe_allow_html=True
)

# Enhanced instructions in sidebar
with st.sidebar.expander("📖 Instructions", expanded=False):
    st.markdown("""
    **How to use:**
    
    1. **Configure API Settings:**
       - Enter your WooWonder site URL
       - Provide a valid access token
       - Enter your server key
    
    2. **Users Data:**
       - Enter user IDs separated by commas
       - Set batch size for large requests
       - Click "Fetch Users Data"
       - Preview, search, and download CSV
    
    3. **Articles Data:**
       - Set your filter options
       - Use quick actions for common requests
       - Click "Fetch Articles"
       - Preview with author info and download CSV
    
    **Enhanced Features:**
    - Author username/email extraction
    - Batch processing for large datasets
    - **Bulk export up to 10,000 records**
    - Automatic pagination for large exports
    - Data search and filtering
    - Column selection for preview
    - Improved error handling
    - Progress tracking
    
    **Tips:**
    - Use smaller limits for initial testing
    - Use "Bulk Export" for large datasets (auto-paginates)
    - Check your API rate limits
    - Verify your access token is valid
    - Use batch processing for large user lists
    - Bulk export handles API 1000-record limit automatically
    """)

# Enhanced status indicators
with st.sidebar.expander("🔧 Tools", expanded=False):
    if st.button("🔍 Test API Connection"):
        with st.spinner("Testing API connection..."):
            test_result = make_api_request('get-articles', {'limit': 1})
            if test_result and test_result.get('api_status') == 200:
                st.success("✅ API connection successful!")
            else:
                st.error("❌ API connection failed. Check your settings.")
    
    if st.button("🧹 Clear Cache"):
        st.cache_data.clear()
        st.success("✅ Cache cleared!")
    
    st.markdown("**API Status:**")
    if all([site_url, access_token, server_key]):
        st.success("✅ Configuration complete")
    else:
        st.warning("⚠️ Configuration incomplete")
