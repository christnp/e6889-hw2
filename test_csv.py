import csv

keys = ["P2_WholeHouse_Power_Val", 
        "DP2_WholeHouse_VAR_Val", "DP2_Condenser_Power_Val", 
        "DP2_Condenser_VAR_Val", "DP2_AirHandler_Power_Val", 
        "DP2_AirHandler_VAR_Val", "DP2_WaterHeater_Power_Val", 
        "DP2_Dryer_Power_Val", "DP2_Range_Power_Val", 
        "DP2_Refrigerator_Power_Val", "DP2_Washer_Power_Val", 
        "DP2_Dishwasher_Power_Val", "DP2_Lights_Power_Val", 
        "DP2_NRecept_Power_Val", "DP2_CounterRecpt_Power_Val", 
        "DP2_WDRecpt_Power_Val", "Timestamp"]

with open('Export_SPL_House2_050216_Data_test.csv') as f:
    first_line = f.readline()
    second_line = f.readline()

def process(element):
    #element_uni = element.encode('utf-8')
    # assume CSV as data input
    try:
        #elements = element_uni.split(",")
        elements = list(csv.reader([element]))[0]
        values = elements[1:-1:2]# +  elements[0] #remove unit and test columns
        values.append(elements[0]) # add timestamp to the end
        # Optimization 1: "merge"      
        #element_dict = dict(zip(self.keys,values))
        yield dict(zip(keys,values))

    except:
        # self.num_parse_errors.inc()
        # logging.error('Parse error on \'%s\'', element)
        print("Failed to parse \'%s\'"%element)

### First Transform (as parse)
# elements = list(csv.reader([second_line]))[0]
# values = elements[1:-1:2]# +  elements[0] #remove unit and test columns
# values.append(elements[0]) # add timestamp to the end

# #### Second Transform (as map)
# element_dict = dict(zip(keys,values))
# lambda: {k: v for k, v in zip(keys, values)})

for i in process(second_line):
   print(i.pop("Timestamp",None))
   print(i)

#elements = elements[1::2]