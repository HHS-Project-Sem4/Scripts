import pandas as pd
from keras.layers import Input, Dense, concatenate, Reshape
from keras.models import Model

order_details_df = pd.read_csv('Datasets/Order_Details.csv')
products_df = pd.read_csv('Datasets/Product.csv')

merged_df = pd.merge(order_details_df, products_df, on='PRODUCT_id')

merged_df = merged_df[['DAY_date', 'ORDER_DETAIL_order_quantity', 'ORDER_DETAIL_unit_price']]

merged_df['DAY_date'] = pd.to_datetime(merged_df['DAY_date'], errors='coerce')
merged_df['year'] = merged_df['DAY_date'].dt.year
merged_df['month'] = merged_df['DAY_date'].dt.month
merged_df['day'] = merged_df['DAY_date'].dt.day
merged_df = merged_df.drop('DAY_date', axis=1)

merged_df = merged_df.dropna()

y_units = merged_df['ORDER_DETAIL_order_quantity'].values
y_prices = merged_df['ORDER_DETAIL_unit_price'].values

X = merged_df[['ORDER_DETAIL_order_quantity', 'ORDER_DETAIL_unit_price', 'year', 'month', 'day']].values

print(X.shape)                    

X[:, 0] = X[:, 0].astype(int)
X[:, 2] = X[:, 2].astype(int)

X = X.reshape((X.shape[0], 1, X.shape[1]))

order_quantity_input = Input(shape=(1, 1))
unit_price_input = Input(shape=(1, 1))
year_input = Input(shape=(1, ))
month_input = Input(shape=(1, ))
day_input = Input(shape=(1, ))

year_input = Reshape((1, 1))(year_input)
month_input = Reshape((1, 1))(month_input)
day_input = Reshape((1, 1))(day_input)

concatenated_inputs = concatenate([order_quantity_input, unit_price_input, year_input, month_input, day_input], axis=-1)

hidden1 = Dense(64, activation='relu')(concatenated_inputs)
hidden2 = Dense(32, activation='relu')(hidden1)

units_output = Dense(1, name='units_output')(hidden2)
prices_output = Dense(1, name='prices_output')(hidden2)

model = Model(inputs=[order_quantity_input, unit_price_input,
                      year_input, month_input, day_input],
              outputs=[units_output, prices_output])

model.compile(optimizer='adam', loss={'units_output': 'mse', 'prices_output': 'mse',},
              metrics={'units_output': 'mae', 'prices_output': 'mae'})

model.fit([X[:, :, 0], X[:, :, 1], X[:, :, 2], X[:, :, 3], X[:, :, 4]],
          [y_units, y_prices],
          epochs=10,
          batch_size=32)

predictions = model.predict([X[:, :, 0], X[:, :, 1], X[:, :, 2], X[:, :, 3], X[:, :, 4]])

predicted_units = predictions[0]
predicted_prices = predictions[1]