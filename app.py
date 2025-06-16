import os
import pandas as pd
from datetime import datetime, timedelta
from jobspy import scrape_jobs
from supabase import create_client
import logging
import time
from dotenv import load_dotenv
from flask import Flask, jsonify

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class JobScraper:
    def __init__(self):
        # Initialize Supabase client
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_ANON_KEY')
        
        if not supabase_url or not supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_ANON_KEY environment variables must be set")
        
        try:
            self.supabase = create_client(supabase_url, supabase_key)
            # Test the connection
            self.supabase.table('job_listings').select("id").limit(1).execute()
            logger.info("Supabase connection successful")
        except Exception as e:
            logger.error(f"Supabase connection failed: {str(e)}")
            raise
        
        # Job search configuration
        self.locations = [
            "Bengaluru, IN",
            "Hyderabad, IN", 
            "New Delhi, IN",
            "Gurgaon, IN",
            "Pune, IN",
            "Mumbai, IN",
            "Chennai, IN"
        ]
        
        self.sites = ["naukri", "linkedin"]
        self.search_term = "product manager"
        self.results_per_location = 10  # Start small for testing
        
    def scrape_jobs_for_location(self, location: str):
        """Scrape jobs for a specific location"""
        try:
            logger.info(f"Scraping jobs for {location}")
            
            jobs = scrape_jobs(
                site_name=self.sites,
                search_term=self.search_term,
                location=location,
                results_wanted=self.results_per_location,
                hours_old=720,
                country_indeed='india'  # Use 'india' instead of 'IN'
            )
            
            if jobs is not None and not jobs.empty:
                logger.info(f"Found {len(jobs)} jobs for {location}")
                return jobs
            else:
                logger.warning(f"No jobs found for {location}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error scraping jobs for {location}: {str(e)}")
            return pd.DataFrame()
    
    def insert_jobs_to_supabase(self, jobs_df):
        """Insert jobs into Supabase table"""
        if jobs_df.empty:
            logger.info("No jobs to insert")
            return
        
        try:
            # Convert DataFrame to list of dictionaries
            jobs_list = jobs_df.to_dict('records')
            
            # Clean the data
            cleaned_jobs = []
            for job in jobs_list:
                cleaned_job = {}
                for key, value in job.items():
                    # Handle pandas NaN values
                    if pd.isna(value):
                        cleaned_job[key] = None
                    # Handle integer fields that come as floats
                    elif key in ['company_reviews_count', 'vacancy_count', 'min_amount', 'max_amount'] and value is not None:
                        try:
                            cleaned_job[key] = int(float(str(value))) if str(value) != 'nan' else None
                        except (ValueError, TypeError):
                            cleaned_job[key] = None
                    # Handle array fields (skills, emails)
                    elif key in ['skills', 'emails'] and value is not None:
                        if isinstance(value, str):
                            # Convert comma-separated string to array
                            cleaned_job[key] = [skill.strip() for skill in value.split(',') if skill.strip()]
                        elif isinstance(value, list):
                            cleaned_job[key] = value
                        else:
                            cleaned_job[key] = None
                    # Handle company rating as numeric
                    elif key == 'company_rating' and value is not None:
                        try:
                            cleaned_job[key] = float(str(value)) if str(value) != 'nan' else None
                        except (ValueError, TypeError):
                            cleaned_job[key] = None
                    else:
                        cleaned_job[key] = str(value) if value is not None else None
                
                # Add timestamp
                cleaned_job['scraped_at'] = datetime.now().isoformat()
                cleaned_jobs.append(cleaned_job)
            
            # Insert in smaller batches
            batch_size = 10
            for i in range(0, len(cleaned_jobs), batch_size):
                batch = cleaned_jobs[i:i + batch_size]
                
                try:
                    # Use upsert to handle duplicates
                    self.supabase.table('job_listings').upsert(
                        batch, 
                        on_conflict='id'
                    ).execute()
                    logger.info(f"Inserted/updated batch of {len(batch)} jobs")
                    time.sleep(2)  # Be gentle with API
                    
                except Exception as batch_error:
                    logger.error(f"Failed to insert batch: {str(batch_error)}")
                    continue
                
        except Exception as e:
            logger.error(f"Error preparing jobs for insertion: {str(e)}")
    
    def run_scrape(self):
        """Main function to run job scraping"""
        logger.info("Starting job scraping...")
        
        total_jobs = 0
        
        # Scrape all locations
        for location in self.locations:
            try:
                jobs_df = self.scrape_jobs_for_location(location)
                if not jobs_df.empty:
                    self.insert_jobs_to_supabase(jobs_df)
                    total_jobs += len(jobs_df)
                
                # Add delay between locations
                time.sleep(10)
                
            except Exception as e:
                logger.error(f"Error processing {location}: {str(e)}")
                continue
        
        logger.info(f"Job scraping completed! Total jobs processed: {total_jobs}")
        return True

# Flask app
app = Flask(__name__)

@app.route('/health')
def health_check():
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/scrape-jobs', methods=['POST', 'GET'])  # Allow both GET and POST
def trigger_scrape():
    try:
        scraper = JobScraper()
        success = scraper.run_scrape()
        
        if success:
            return jsonify({
                "status": "success", 
                "message": "Job scraping completed",
                "timestamp": datetime.now().isoformat()
            })
        else:
            return jsonify({
                "status": "error",
                "message": "Job scraping failed - check logs",
                "timestamp": datetime.now().isoformat()
            }), 500
            
    except Exception as e:
        logger.error(f"Error in scrape trigger: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Error: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000)))