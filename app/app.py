import os
import io
import base64
import sys
import traceback

os.chdir(r"T:\Intern Folder\External Research\2023 Interns\BPIPE")
sys.path.append(r"T:\Intern Folder\External Research\2023 Interns\BPIPE")

from dash import Dash, dcc, html, Input, Output, State, dash_table
import matplotlib
import matplotlib.pyplot as plt

matplotlib.use("Agg")  # Avoid TK gui toolkit causing error
from msg_parsing import OrderBook
from charting import gross_buy_sell
import pandas as pd


def encode_img(img_path: str):
    if os.path.exists(img_path):
        with open(img_path, "rb") as f:
            data = base64.b64encode(f.read()).decode("utf8")
    else:
        data = ""
    return data


app = Dash(__name__)

df = pd.DataFrame(columns=["Name", "Mark"])

app.layout = html.Div(
    [
        html.H1("Order Book Recap", style={"textAlign": "center"}),
        html.Div(
            ["Ticker Input: ", dcc.Input(id="ticker", value=" HK Equity", type="text", style={"textAlign": "center"}), html.Button("Generate", id="btn", n_clicks=0, style={"textAlign": "center"})],
            style={"display": "flex", "justifyContent": "center"},
        ),
        html.Img(id="gross_buy_sell_chart", style={"textAlign": "center"}),
        html.Br(),
        dash_table.DataTable(
            id="journal_df_tbl",
            fill_width=False,
            style_data_conditional=[
                {
                    "if": {"filter_query": "{Action} = Trade"},  # Hightlight "Trades"
                    "backgroundColor": "rgb(220, 220, 220)",
                }
            ],
        ),
    ]
)


@app.callback(Output(component_id="journal_df_tbl", component_property="data"), Output(component_id="gross_buy_sell_chart", component_property="src"), Input(component_id="btn", component_property="n_clicks"), State(component_id="ticker", component_property="value"), prevent_initial_call=True)
def update_charts(n_clicks, ticker):
    obj = OrderBook(ticker)
    obj.run()

    try:  # Price Chart
        with io.BytesIO() as buffer:
            fig3 = gross_buy_sell(obj.journal_df)
            plt.savefig(buffer, format="png", bbox_inches="tight")
            plt.close(fig3)
            data2 = base64.b64encode(buffer.getbuffer()).decode("utf8")
    except Exception:
        print(traceback.format_exc())
        data2 = ""

    return obj.journal_df.to_dict("records"), f"data:image/png;base64,{data2}"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=1234, debug=True, dev_tools_hot_reload=False)
