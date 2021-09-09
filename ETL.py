from lib import  *

def process_data(path_dir , func, func2):
    # checking current working directory
    print(f"Current working directory : {os.getcwd()}")
    current_dir = os.getcwd()
    # Get current folder and subfolder event data
    filepath = current_dir +  path_dir

    # Create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):
        # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root, '*-events.csv'))
    print(file_path_list)
    # collect data procoess
    func(file_path_list, func2)

def  data_collect(file_path_list, func2  ):

    # initiating empty list  rows that will be generated  data
    full_data_rows = []
    num_files = len(file_path_list)
    for  i, file in enumerate(file_path_list,1):
        print(f"{i}/{num_files} file processed")
       # read csv file in  event data dir
        with open(file,'r', encoding='utf8', newline= '') as csvfile:

            csvreader = csv.reader(csvfile)
            # first line is  table header so  we should use next()
            next(csvreader)
            # extract line by line
            for line  in csvreader:
                full_data_rows.append(line)
    print(f"Total rows: {len(full_data_rows)}")
    print(f"Sample data : \n {full_data_rows[:5]}")

    func2(full_data_rows)


def export_data(full_data_rows):
    """
    creating a smaller event data csv file  called event_datafile_full csv that will be
    inser into apache cassandra
    """
    csv.register_dialect('myDialect',quoting = csv.QUOTE_ALL, skipinitialspace = True)

    # create column  csv
    column = ['user_id','firstname', 'lastname','gender',\
              'iteminsession','session_id', 'length','level', 'location','artist','song' ]


    with open ('event_datefile_new.csv','w', encoding='utf8',\
               newline= '') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(column)
        for  row in full_data_rows:
            if row[0] == '':
                continue
            writer.writerow((row[16],row[2],row[5],row[3],row[4],row[12],row[6],row[7],row[8],row[0], row[13])
)
    with open('event_datefile_new.csv', 'r', encoding='utf8', newline='') as f :
        print(sum(1 for line in f ) )


def main():
    process_data( path_dir='/event_data', func= data_collect, func2=  export_data)

if __name__ ==  "__main__":
    main()
    print("finished processing")