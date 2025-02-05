import pandas as pd
from shiny import App, ui, render, reactive
import shiny

# Path to the CSV file
csvfn = 'reviewer_metrics.csv'

# Load the CSV into a pandas DataFrame
def load_df():
    return pd.read_csv(csvfn)

# Save the updated DataFrame to the CSV
def save_df(df):
    df.to_csv(csvfn, index=False)

# UI setup
app_ui = ui.page_fluid(
    ui.input_text("text_search", "Search by MS Number only", ""),
    ui.output_text("search_results"),
    ui.h3("Data"),
    ui.output_table("data_table"),
    ui.input_action_button("save_button", "Save")
)

# Server logic
def server(input, output, session):

    # Reactive value to hold the DataFrame
    @reactive.Calc
    def filtered_data():
        df = load_df()
        search_text = input.text_search.get()  # Get the value from the reactive input
        if search_text:
            # Ensure that the search_text is a string before using it in str.contains
            search_text = str(search_text)
            df = df[df["MS Number"].str.contains(search_text, na=False)]
        return df

    # Render the filtered results based on search
    @output()
    @render.text
    def search_results():
        df = filtered_data()
        return f"Found {len(df)} results" if input.text_search.get() else "No search performed."

    # Render the data table
    @output()
    @render.table
    def data_table():
        return filtered_data()

    # Handle Save button
    @reactive.Effect
    def save_data():
        if input.save_button.get() > 0:  # Access the value of the reactive input using `.get()`
            df = filtered_data()
            save_df(df)

# Create the Shiny app
app = App(app_ui, server)

# Run the app
if __name__ == "__main__":
    app.run()
