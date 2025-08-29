import pandas as pd
import geopandas as gpd
from shapely.geometry import shape, Point
from matplotlib.colors import LinearSegmentedColormap
import matplotlib.pyplot as plt
from geopy.geocoders import Nominatim
from tqdm import tqdm
tqdm.pandas()
import time

class CentroDataProcessor:
    def __init__(self, data_path='data/magyar_petered_telepulesek.xlsx', shapefile_path='data/kozighatarok/admin8.shp'):
        self.data_path = data_path
        self.shapefile_path = shapefile_path
        self.df = None
        self.gdf = None

    def load_shapefile(self,):
        self.shapefile = gpd.read_file(self.shapefile_path)
        print("Shapefile loaded successfully.")

    def load_data(self):
        self.df = pd.read_excel(self.data_path, header=0)
        print("Data loaded successfully.")

    def preprocess_data(self):
        self.df.columns=['munis']
        self.df['is_mped']=1
        print("Data preprocessed successfully.")

    def merge_data(self):
        self.gdf = pd.merge(left=self.shapefile, right=self.df, left_on='NAME', right_on='munis', how='left')
        self.gdf=self.gdf.drop(columns=['munis', 'ADMIN_LEVE']).rename(columns={'NAME':'name'})
        self.gdf['is_mped']=self.gdf.is_mped.fillna(0).astype(int)
        print("Data merged successfully.")

    def visualize_data(self):
        cmap = LinearSegmentedColormap.from_list('custom_cmap', ['green', 'red']) #['#FF0000', '#008000']
        self.gdf.boundary.plot(ax=plt.gca(), color='black', linewidth=0.1)
        self.gdf.plot(column='is_mped', cmap=cmap, legend=True, ax=plt.gca())
        plt.show()
        print("Data visualised successfully.")

    def geocode_address(self, address):
        geolocator = Nominatim(user_agent="is_magyar_petered", timeout=10)
        location = geolocator.geocode(address)
        if location:
            return Point(location.longitude, location.latitude)
        else:
            return None

    def add_geocoded_columns(self, limit=10):
        if limit!='no':
            self.gdf=self.gdf.sample(just_first_n)
        vmi = self.gdf['name'].progress_apply(
            lambda x: self.geocode_address(x))
        self.gdf['centro'] = vmi
        print("Geocoded column added successfully.")

    def save_processed_data(self, output_path):
        self.gdf.to_file(output_path, driver='GeoJSON')
        print(f"Processed data saved to {output_path}.")

if __name__ == "__main__":
    output_path = "data/munis_with_centro.geojson" 

    processor = CentroDataProcessor()
    processor.load_shapefile()
    processor.load_data()
    processor.preprocess_data()
    processor.merge_data()
    # processor.visualize_data()
    processor.add_geocoded_columns(limit='no') # Use 'no' to process all entries
    processor.save_processed_data(output_path)