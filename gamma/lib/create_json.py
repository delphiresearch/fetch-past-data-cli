import json
import os

def create_json_file(data, filename):
    """
    Function to save JSON data as a file
    
    Args:
        data: Python object that can be converted to JSON format
        filename: Name of the JSON file to create (.json extension will be added automatically)
    """
    # Check if data is an empty array
    if isinstance(data, list) and len(data) == 0:
        print("Error: Data is an empty array. JSON file was not created.")
        return
    
    # Add .json extension if not present
    if not filename.endswith('.json'):
        filename += '.json'
    
    # Create file in the same directory as the script
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    file_path = os.path.join(project_root, "output", filename)
    
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"JSON file successfully created: {file_path}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

