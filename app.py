import streamlit as st
import pandas as pd

st.title("weatheranalys app")
st.write("Let's start!")

# def process_main_page():
#     show_main_page()
#     process_side_bar_inputs()

# def show_main_page():
#     image = Image.open('data/titanic.jpg')

#     st.set_page_config(
#         layout="wide",
#         initial_sidebar_state="auto",
#         page_title="Demo Titanic",
#         page_icon=image,

#     )

#     st.write(
#         """
#         # Классификация пассажиров титаника
#         Определяем, кто из пассажиров выживет, а кто – нет.
#         """
#     )

#     st.image(image)


# def write_user_data(df):
#     st.write("## Ваши данные")
#     st.write(df)


# def write_prediction(prediction, prediction_probas):
#     st.write("## Предсказание")
#     st.write(prediction)

#     st.write("## Вероятность предсказания")
#     st.write(prediction_probas)



# if __name__ == "__main__":
#     process_main_page()