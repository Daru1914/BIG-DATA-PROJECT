"""
Pres creation
"""
import os
import streamlit as st
import pandas as pd
import altair as alt

rests = pd.read_csv("data/restaurants.csv")
menus = pd.read_csv("data/restaurant-menus.csv")
q1 = pd.read_csv("output/q1.csv")
q2 = pd.read_csv("output/q2.csv")
# q3 = pd.read_csv("output/q3.csv")
q4 = pd.read_csv("output/q4.csv")
q5 = pd.read_csv("output/q5.csv")
q6 = pd.read_csv("output/q6.csv", sep='\t')
q7 = pd.read_csv("output/q7.csv")
q8 = pd.read_csv("output/q8.csv")
q9 = pd.read_csv("output/q9.csv")
q110 = pd.read_csv("output/q10.csv")

st.write("# Big Data Project  \n ## ♿ Food Price Prediction 🤡  \n", "*Year*: **2023**\
 \n" "*Team*: **Nikolay Pavlenko, Ashera Dyussenova** \n")
st.header("Data Characteristics")
# Display the descriptive information of the dataframe
# rests_cols = rests.dtypes
rest_desc = rests.describe()
menu_desc = menus.describe()
# st.write(rests_cols)
st.write("This dataset contains over 40,000 Restaurants and their\
          menus partnered with Uber Eats in the USA. It's split into\
          two CSV files: 'restaurants.csv' with info on restaurant's\
          unique ID, position, name, score, ratings, category, price range,\
          full address, zip code, latitude, and longitude. And\
          'restaurant-menus.csv' with over 3.71 million menu entries,\
          including the restaurant ID, menu category, name, description, and price.\
          It's a valuable resource for food enthusiasts, researchers, and industry analysts.")
st.subheader("restaurants.csv")
st.write("""restaurants.csv (40k+ entries, 11 columns) \n
id (Restaurant id) - int\n
position (Restaurant position in the search result) - int\n
name (Restaurant name) - str\n
score (Restaurant score) - float\n
ratings (Ratings count) - int\n
category (Restaurant category) - str\n
price_range (Restaurant price range - $ = Inexpensive, $$ = Moderately expensive, 
$$$ = Expensive, $$$$ = Very Expensive) - Source - stackoverflow - str\n
full_address (Restaurant full address) - str\n
zip_code (Zip code) - str\n
lat (Latitude)- str\n
long (Longitude) - str\n""")
st.write(rest_desc)

st.subheader("menus.csv")
st.write("""restaurant-menus.csv (3.71M entries, 5 columns)\n
restaurant_id (Restaurant id) - int\n
category (Menu category) - str\n
name (Menu Name) - str\n
description (Menu description) - str\n
price (Menu price) - str\n""")
st.write(menu_desc)

st.subheader('Some samples from the data')
st.markdown('`restaurants` table')
st.write(rests.head(5))
st.markdown("`restaurant-menus` table")
st.write(menus.head(5))


st.header("EDA and Insights")
st.write("We ran several queries against our data to better understand it and \
         were able to gather some insights into it.")

st.subheader("1.Count the number of restaurants in each category")
chart = alt.Chart(q1.sort_values(by='num_restaurants', ascending=False).head(10))\
    .mark_circle().encode(
    x='category:N',
    y='num_restaurants:Q'
)
# Display the chart
# chart.show()


def clicked():
    """
    If you click, stuff happens
    """
    chart.show()

def unclicked():
    """
    If you don't click, stuff happens
    """
    print(".")
    # st.balloons()

if st.button('Show picture'):
    clicked()
else:
    unclicked()

st.code("""SELECT category, COUNT(*) AS num_restaurants
FROM restaurants
GROUP BY category
ORDER BY num_restaurants DESC;""", language='sql')

st.write("Top 10 categories by restaurant number. We can see that\
         traditional American and Mexican restaurants are the most popular.")



st.subheader("2.Find the top-rated restaurants in each category")
chart = alt.Chart(q2).mark_circle().encode(
    x='category', y='name', size='score', color='score',
    tooltip=['category', 'name', 'score'])
# Display the chart
# chart.show()

if st.button('Show picture '):
    clicked()
else:
    unclicked()

st.code("""SELECT category, name, score
FROM restaurants
WHERE score IS NOT NULL
ORDER BY score DESC
LIMIT 10;""", language='sql')

st.write("On the chart we can see the best restaurants in their categories.")



st.subheader("3.Find the most popular menu items across all restaurants")
chart = alt.Chart(q4)\
    .mark_circle().encode(
    x='name:N',
    y='num_occurences:Q'
)
# Display the chart
# chart.show()

if st.button('Show picturе '):
    clicked()
else:
    unclicked()

st.code("""SELECT name, COUNT(*) AS num_occurrences
FROM menus
GROUP BY name
ORDER BY num_occurrences DESC
LIMIT 10;""", language='sql')

st.write("On the chart we can see the most popular foods in America - \
         bottled water and French fries are by far the most popular foods.")



st.subheader("4.Find the number of restaurants in each zip code")
chart = alt.Chart(q5.iloc[2:120])\
    .mark_circle().encode(
    x='zip_code:Q',
    y=' num_restaurants:Q'
)
# Display the chart
# chart.show()

if st.button('Show picturе'):
    clicked()
else:
    unclicked()

st.code("""SELECT zip_code, COUNT(*) AS num_restaurants
FROM restaurants
GROUP BY zip_code
ORDER BY num_restaurants DESC;""", language='sql')

st.write("On the chart we can see that most of the restaurants are clustered\
         together in close proximity to each other, as similar zip indices suggest.")



st.subheader("5.Names of restaurants with the word \"pizza\" in the food")
# Display the chart
# chart.show()

if st.button('Show names:'):
    st.write(q6.sample(10))
else:
    unclicked()

st.code("""SELECT DISTINCT r.name
FROM restaurants r
JOIN menus m ON r.id = m.restaurant_id
WHERE LOWER(m.name) LIKE '%pizza%';""", language='sql')

st.write("We get a sample of places you can get a pizza at in the US\
          - not all of them are Italian.")



st.subheader("6.Find the average price of menu items for each category at a given restaurant")
# Display the chart
# chart.show()
chart = alt.Chart(q7).mark_circle().encode(
    x='category', y='name', size='avg_price', color='avg_price',
    tooltip=['category', 'name', 'avg_price'])

if st.button('Show pic:'):
    clicked()
else:
    unclicked()

st.code("""SELECT r.name, m.category, AVG(CAST(REGEXP_REPLACE\
(m.price, ' USD', '') AS FLOAT)) AS avg_price                                                                                                                  
FROM restaurants r                                                                                                                                                  
JOIN menus m ON r.id = m.restaurant_id                                                                                                                             
WHERE r.name = 'Golden Temple Vegetarian Cafe'                                                                                                                    
GROUP BY r.name, m.category;""", language='sql')

st.write("We get a sample of prices for specific food stuffs at\
         Golden Temple Vegetarian Cafe.")



st.subheader("7.Get the top 5 most common menu categories across all restaurants")
# Display the chart
# chart.show()
chart = alt.Chart(q8)\
    .mark_circle().encode(
    x='category',
    y='count:Q'
)

if st.button('Show piсture:'):
    clicked()
else:
    unclicked()

st.code("""SELECT m.category, COUNT(*) AS count
FROM restaurants r
JOIN menus m ON r.id = m.restaurant_id
GROUP BY m.category
ORDER BY count DESC
LIMIT 5;""", language='sql')

st.write("We can see that most restaurants in America pick for you.")



st.subheader("8.Find the restaurants with the highest average rating\
              that have at least one menu item with the word \"burger\"\
              in the name")
# Display the chart
# chart.show()
chart = alt.Chart(q9.head(10))\
    .mark_circle().encode(
    x='name',
    y='avg_rating:Q'
)

if st.button('Show piсture:   '):
    clicked()
else:
    unclicked()

st.code("""SELECT r.name, AVG(CAST(r.score AS FLOAT)) AS avg_rating
FROM restaurants r
JOIN menus m ON r.id = m.restaurant_id
WHERE LOWER(m.name) LIKE '%burger%'
GROUP BY r.name
ORDER BY avg_rating DESC;
""", language='sql')

st.write("Among the top-rated burger restaurants in America we can see both popular\
         international brands and smaller lesser-known ones.")



st.subheader("9.Get the top 10 most expensive menu items across all restaurants")
# Display the chart
# chart.show()
chart = alt.Chart(q110).mark_circle().encode(
    x='restaurant_name', y='dish_name', size='price', color='price',
    tooltip=['restaurant_name', 'dish_name', 'price'])

if st.button('Show piсture:  '):
    clicked()
else:
    unclicked()

st.code("""
SELECT r.name, m.name, m.price
FROM restaurants r
JOIN menus m ON r.id = m.restaurant_id
ORDER BY CAST(REGEXP_REPLACE(m.price, 'USD', '') AS FLOAT) DESC
LIMIT 10;
""", language='sql')

st.write("Due to the limit of 10 queries there is really not much of an insight we can get\
         besides the fact that there is a person who decided to name his restaurant\
         \"buybuy BABY\"👶💩.")

st.header("Models' Performance Results")

st.write("Performing 4-fold cross-validation with grid search, we have found the following\
         hyperparameters for the best models:")
st.table(pd.DataFrame([['Linear Regression', "regParam = 0.01", "elasticNetParam = 0"],
                       ['Decision Tree Regressor',  "maxDepth = 10", "minInstancesPerNode = 1"]],
                       columns = ['Model','Par1', 'Par2']))

st.write("Applying the models to the testing dataset, we have achieved the following\
         RMSE and R2 values:")
st.table(pd.DataFrame([['Linear Regression',9.90,0.19],
                       ['Decision Tree Regressor',8.29,0.42]],
                       columns = ['Model','RMSE','R2']))

st.write("Judging those values, we can conclude that both models perform pretty poorly,\
         as they can't explain the variance in the labeled column, though Decision Tree\
         Regressor does that better, achieveing smaller RMSE and higher R2. This\
         indicates a non-linear relationship between the features and the label.")

lr_predictions = pd.read_csv("output/lr_pred/%s" % os.listdir("output/lr_pred")[0])
dtr_predictions = pd.read_csv("output/dtr_pred/%s" % os.listdir("output/dtr_pred")[0])

st.header("Model predictions")

st.subheader("LR predictions")
st.table(lr_predictions.head(10))

st.subheader("DTR predictions")
st.table(dtr_predictions.head(10))
