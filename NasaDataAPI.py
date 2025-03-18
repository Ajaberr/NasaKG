import requests
import json

# ---------------------------------------------------------------------
# 1) NASA Common Metadata Repository (CMR) - Fetch a Single "Page"
# ---------------------------------------------------------------------
def fetch_nasa_cmr_one_page():
    """
    Fetches up to 10 dataset 'collections' from NASA's CMR, 
    and writes them to a local JSON file named 'nasa_cmr.json'.
    """
    cmr_url = "https://cmr.earthdata.nasa.gov/search/collections.json"
    
    params = {
        "page_size": 10,  # pull 10 results
        "page_num": 1
        # You could add more query filters if desired, e.g.:
        # "keyword": "SMAP" or "platform": "Terra"
    }
    
    try:
        response = requests.get(cmr_url, params=params)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching NASA CMR data: {e}")
        return
    
    with open("nasa_cmr.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    
    print("Saved one page of NASA CMR data to nasa_cmr.json")


# ---------------------------------------------------------------------
# 2) NASA Open Data Portal - Meteorite Landings (Sample)
# ---------------------------------------------------------------------
def fetch_nasa_meteorites_one_page():
    """
    Fetches up to 10 meteorite records from the NASA Open Data Portal.
    Writes the JSON response to 'nasa_meteorites.json'.
    
    Endpoint used: 
      https://data.nasa.gov/resource/y77d-th95.json
    with a limit parameter to fetch fewer records.
    """
    # NOTE: 'y77d-th95' is the resource ID for "Meteorite Landings" dataset on data.nasa.gov
    url = "https://data.nasa.gov/resource/y77d-th95.json"
    
    # We can limit how many records we pull with the $limit parameter:
    params = {
        "$limit": 10
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching NASA Meteorite data: {e}")
        return
    
    with open("nasa_meteorites.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    
    print("Saved one page of NASA meteorite data to nasa_meteorites.json")


def main():
    # 1) Fetch from NASA CMR
    fetch_nasa_cmr_one_page()
    
    # 2) Fetch from NASA's Open Data Portal (meteorite dataset)
    fetch_nasa_meteorites_one_page()


if __name__ == "__main__":
    main()
