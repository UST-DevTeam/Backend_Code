from base import *


def regexspecialchar(original_string):
    original_string=original_string.replace("(", "\\(").replace(")", "\\)")
    return original_string




def generate_unique_id():
    timestamp = int(time.time() * 1000)  
    random_num = random.randint(0, 100)
    random_num1 = random.randint(101, 200) 
    unique_id = f"{timestamp}-{random_num}-{random_num1}"
    print(unique_id,"123456781234567891235789")
    return unique_id

# Example usage:
