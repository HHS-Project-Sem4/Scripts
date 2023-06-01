import pandas as pd
from sklearn.preprocessing import LabelEncoder
from keras.layers import Input, Dense, concatenate, Reshape
from keras.models import Model

order_details_df = pd.read_csv('Datasets/Order_Details.csv')
products_df = pd.read_csv('Datasets/Product.csv')

merged_df = pd.merge(order_details_df, products_df, on='PRODUCT_id')

merged_df = merged_df[['DAY_date', 'ORDER_DETAIL_order_quantity', 'ORDER_DETAIL_unit_price',
                       'PRODUCT_category', 'PRODUCT_colour', 'PRODUCT_id']]

merged_df['DAY_date'] = pd.to_datetime(merged_df['DAY_date'], errors='coerce')
merged_df['year'] = merged_df['DAY_date'].dt.year
merged_df['month'] = merged_df['DAY_date'].dt.month
merged_df['day'] = merged_df['DAY_date'].dt.day
merged_df = merged_df.drop('DAY_date', axis=1)

y_units = merged_df['ORDER_DETAIL_order_quantity'].values
y_prices = merged_df['ORDER_DETAIL_unit_price'].values
y_category = merged_df['PRODUCT_category'].values
y_colour = merged_df['PRODUCT_colour'].values
y_product_id = merged_df['PRODUCT_id'].values

X = merged_df[['ORDER_DETAIL_order_quantity', 'ORDER_DETAIL_unit_price', 'PRODUCT_category',
                    'PRODUCT_colour', 'PRODUCT_id', 'year', 'month', 'day']].values

print(X.shape)                    

label_encoder_category = LabelEncoder()
X[:, 2] = label_encoder_category.fit_transform(X[:, 2])

label_encoder_colour = LabelEncoder()
X[:, 3] = label_encoder_colour.fit_transform(X[:, 3])

label_encoder_product_id = LabelEncoder()
X[:, 4] = label_encoder_product_id.fit_transform(X[:, 4])

X[:, 0] = X[:, 0].astype(int)
X[:, 1] = X[:, 1].astype(int)
X[:, 2] = X[:, 2].astype(int)

X = X.reshape((X.shape[0], 1, X.shape[1]))

order_quantity_input = Input(shape=(1, 1))
unit_price_input = Input(shape=(1, 1))
category_input = Input(shape=(1, ))
colour_input = Input(shape=(1, ))
product_id_input = Input(shape=(1, ))
year_input = Input(shape=(1, ))
month_input = Input(shape=(1, ))
day_input = Input(shape=(1, ))

category_input = Reshape((1, 1))(category_input)
colour_input = Reshape((1, 1))(colour_input)
product_id_input = Reshape((1, 1))(product_id_input)
year_input = Reshape((1, 1))(year_input)
month_input = Reshape((1, 1))(month_input)
day_input = Reshape((1, 1))(day_input)

concatenated_inputs = concatenate([order_quantity_input, unit_price_input, category_input,
                                    colour_input, product_id_input, year_input, month_input, day_input], axis=-1)

hidden1 = Dense(64, activation='relu')(concatenated_inputs)
hidden2 = Dense(32, activation='relu')(hidden1)

units_output = Dense(1, name='units_output')(hidden2)
prices_output = Dense(1, name='prices_output')(hidden2)
category_output = Dense(len(label_encoder_category.classes_), activation='softmax', name='category_output')(hidden2)
colour_output = Dense(len(label_encoder_colour.classes_), activation='softmax', name='colour_output')(hidden2)
product_id_output = Dense(len(label_encoder_product_id.classes_), name='product_id_output')(hidden2)

model = Model(inputs=[order_quantity_input, unit_price_input, category_input, colour_input, product_id_input,
                      year_input, month_input, day_input],
              outputs=[units_output, prices_output, category_output, colour_output, product_id_output])

model.compile(optimizer='adam', loss={'units_output': 'mse', 'prices_output': 'mse',
                                      'category_output': 'sparse_categorical_crossentropy',
                                      'colour_output': 'sparse_categorical_crossentropy',
                                      'product_id_output': 'sparse_categorical_crossentropy'},
              metrics={'units_output': 'mae', 'prices_output': 'mae'})

model.fit({'order_quantity_input': X[:, :, 0],
           'unit_price_input': X[:, :, 1],
           'category_input': X[:, :, 2],
           'colour_input': X[:, :, 3],
           'product_id_input': X[:, :, 4],
           'year_input': X[:, :, 5],
           'month_input': X[:, :, 6],
           'day_input': X[:, :, 7]},
          {'units_output': y_units, 'prices_output': y_prices, 'category_output': y_category,
           'colour_output': y_colour, 'product_id_output': y_product_id},
          epochs=10, batch_size=32)

predictions = model.predict({'order_quantity_input': X[:, :, 0],
                             'unit_price_input': X[:, :, 1],
                             'category_input': X[:, :, 2],
                             'colour_input': X[:, :, 3],
                             'product_id_input': X[:, :, 4],
                             'year_input': X[:, :, 5],
                             'month_input': X[:, :, 6],
                             'day_input': X[:, :, 7]})

predicted_units = predictions[0]
predicted_prices = predictions[1]
predicted_category = predictions[2]
predicted_colour = predictions[3]
predicted_product_id = predictions[4]