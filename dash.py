import dash
# import dash_html_components as html
from dash import html
# import dash_core_components as dcc
from dash import dcc
import plotly.graph_objects as go
import plotly.express as px
import verticapy

app = dash.Dash()  # initialising dash app
df = px.data.stocks()  # reading stock price dataset


def stock_prices():
    # Function for creating line chart showing Google stock prices over time
    fig = go.Figure([
        go.Scatter(
            x=df['date'],
            y=df['GOOG'],
            line=dict(color='firebrick', width=4),
            name='Google',
        )
    ])
    fig.update_layout(
        title='Prices over time',
        xaxis_title='Dates',
        yaxis_title='Prices',
    )
    return fig


app.layout = html.Div(
    id='parent',
    children=[
        html.H1(
            id='H1',
            children='Test',
            style={
                'textAlign': 'center',
                'marginTop': 30,
                'marginBottom': 40,
            },
        ),
        dcc.Graph(
            id='line_plot',
            figure=stock_prices(),
        ),
        dcc.Dropdown(
            id='dropdown',
            options=[
                {'label': 'Google', 'value': 'GOOG'},
                {'label': 'Apple', 'value': 'AAPL'},
                {'label': 'Amazon', 'value': 'AMZN'},
            ],
            value='GOOG'
        )
    ]
)



app.run_server()