with open("tpch_queries_original.sql", 'r') as file:
    data = file.read()
    print(data)

with open("tpch_queries_original.sql", 'w') as file:
    file.write(data.replace("\n", " "))
    