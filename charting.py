import matplotlib.pyplot as plt
import pandas as pd

# Load data and aggregate the volume for both sides

def gross_buy_sell(data: pd.DataFrame):
    data = data[data['Action'] == 'Trade'] # Filter trade
    group_broker = data.groupby(by=["Broker Name", 'Side'])['Size'].sum()/1000
    group_broker = group_broker.unstack(level=-1)
    group_broker = group_broker.fillna(0).sort_values(by=group_broker.columns[0], ascending=False)

    # Plotting
    hfont = {'fontname':'Calibri'}
    facecolor = 'white'
    color_red = '#c14e4e'
    color_blue = '#99C4E7'
    index = group_broker.index
    if group_broker.shape[1] == 2:
        column0 = group_broker['ASK']
        column1 = group_broker['BID']
        title0 = 'Ask, thousand'
        title1 = 'Bid, thousand'
    elif group_broker.shape[1] == 1 and group_broker.columns[0] == "ASK":
        column0 = group_broker['ASK']
        title0 = 'Ask, thousand'
    elif group_broker.shape[1] == 1 and group_broker.columns[0] == "BID":
        column0 = group_broker['BID']
        title0 = 'Bid, thousand'

    if group_broker.shape[1] == 2:
        fig, axes = plt.subplots(figsize=(10,15), facecolor=facecolor, ncols=2, sharey=True)
        fig.tight_layout() 
        axes[0].barh(index, column1, align='center', color=color_blue, alpha=0.8, zorder=10)
        axes[0].set_title(title1, fontsize=18, pad=15, color=color_blue, alpha=0.8, **hfont)
        axes[1].barh(index, column0, align='center', color=color_red, alpha=0.8, zorder=10)
        axes[1].set_title(title0, fontsize=18, pad=15, color=color_red, alpha=0.8, **hfont)

        axes[0].invert_xaxis() 
        for i, v in enumerate(column0):
            axes[1].text(v/2, i, str(int(v)), color='black', ha='right', va='center', zorder=11)
        for i, v in enumerate(column1):
            axes[0].text(v/2, i, str(int(v)), color='black', ha='left', va='center', zorder=11)
        plt.gca().invert_yaxis()
        axes[0].set(yticks=group_broker.index, yticklabels=group_broker.index)
        axes[0].yaxis.tick_left()
        axes[0].tick_params(axis='y', colors='black') # tick color
    if group_broker.shape[1] == 1:
        fig, axes = plt.subplots(figsize=(10,15), facecolor=facecolor, ncols=1)
        fig.tight_layout() 
        if group_broker.columns[0] == "ASK":
            axes.barh(index, column0, align='center', color=color_blue, alpha=0.8, zorder=10)
            axes.set_title(title0, fontsize=18, pad=15, color=color_blue, alpha=0.8, **hfont)
        if group_broker.columns[0] == "BID":
            axes.barh(index, column0, align='center', color=color_red, alpha=0.8, zorder=10)
            axes.set_title(title0, fontsize=18, pad=15, color=color_red, alpha=0.8, **hfont)
        for i, v in enumerate(column0):
            axes.text(v/2, i, str(int(v)), color='black', ha='left', va='center', zorder=11)
        plt.gca().invert_yaxis()
        axes.set(yticks=group_broker.index, yticklabels=group_broker.index)
        axes.yaxis.tick_left()
        axes.tick_params(axis='y', colors='black') # tick color

    return fig