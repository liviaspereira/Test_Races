engine = create_engine("postgresql://postgres:123@localhost:5432/postgres")

data = []
f = open("dados.csv", "r")
lines = f.readlines()
count = 1
for line in lines:
    if count == 1:
        keys = [bad_line.strip() for bad_line in line.split(",")]
    else:
        dicionario = {}
        dados = [bad_line.strip() for bad_line in line.split(",")]
        for i in range(len(keys)):
            dicionario[keys[i]] = dados[i]
        data.append(dicionario)
    count += 1

with engine.connect() as con:
    statement = text("""INSERT INTO races(vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, rate_code, store_and_fwd_flag, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, imp_surcharge, total_amount, pickup_location_id, dropoff_location_id) VALUES(:vendor_id, :pickup_datetime, :dropoff_datetime, :passenger_count, :trip_distance, :rate_code, :store_and_fwd_flag, :payment_type, :fare_amount, :extra, :mta_tax, :tip_amount, :tolls_amount, :imp_surcharge, :total_amount, :pickup_location_id, :dropoff_location_id)""")
    for line in data:
        con.execute(statement, **line)