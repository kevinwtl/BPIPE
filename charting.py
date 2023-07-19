import matplotlib.pyplot as plt
import pandas as pd

def formatted_int(number):
    """
    format values per thousands : K-thousands, M-millions, B-billions.

    parameters:
    -----------
    number is the number you want to format

    """
    if number != 0:
        for unit in ['','K','M', 'B']:
            if abs(number) < 1000:
                if float(number) == int(number):
                    return f"{int(number)}{unit}"
                else:
                    return f"{number}{unit}"
            number /= 1000
        return f"{number:6.2f}"
    else:
        return ''

# Load data and aggregate the volume for both sides

def gross_buy_sell(data: pd.DataFrame, trade: pd.DataFrame):

    
    data = data[data['Action'] == 'Trade'] # Filter trade
    group_broker = data.groupby(by=["Broker Name", 'Side'])['Size'].sum()
    group_broker = group_broker.unstack(level=-1)
    group_broker = group_broker.fillna(0).sort_values(by=group_broker.columns[0], ascending=False)

    total_vol = trade[(trade['conditionCodes'] != 'IE') & (trade['conditionCodes'] != 'U')]['size'].sum()

    # Plotting
    hfont = {'fontname':'Calibri'}
    facecolor = 'white'
    color_red = '#c14e4e'
    color_blue = '#99C4E7'
    _, col_len = group_broker.shape


    # TODO: set title str before if-loop


    if col_len == 2: # TODO: rewrite the conditions, to be more intuituive
        title0 = f'Ask \n Total Volume: {int(total_vol):,} \n Volume Mapped: {int(group_broker.sum()[0]):,} \n Percentage Mapped: {int(group_broker.sum()[0])/int(total_vol):.2%}' # TODO: Rename var, take out 0, 1 etc.
        title1 = f'Bid \n Total Volume: {int(total_vol):,} \n Volume Mapped: {int(group_broker.sum()[1]):,} \n Percentage Mapped: {int(group_broker.sum()[1])/int(total_vol):.2%}'
        unmapped = pd.DataFrame({'ASK': total_vol  - group_broker.sum()[0], 'BID':  total_vol  - group_broker.sum()[1]}, index=['Unmapped Brokers'])
        group_broker = pd.concat([group_broker, unmapped])
        column0 = group_broker['ASK']
        column1 = group_broker['BID']
        index = group_broker.index
    elif col_len == 1 and group_broker.columns[0] == "ASK":
        title0 = f'Ask \n Total Volume: {int(total_vol):,} \n Volume Mapped: {int(group_broker.sum()[0]):,} \n Percentage Mapped: {int(group_broker.sum()[0])/int(total_vol):.2%}'
        unmapped = pd.DataFrame({'ASK': total_vol  - group_broker.sum()[0]}, index=['Unmapped Brokers'])
        group_broker = pd.concat([group_broker, unmapped])
        column0 = group_broker['ASK']
        index = group_broker.index
    elif col_len == 1 and group_broker.columns[0] == "BID":
        title0 = f'Bid \n Total Volume: {int(total_vol):,} \n Volume Mapped: {int(group_broker.sum()[0]):,} \n Percentage Mapped: {int(group_broker.sum()[0])/int(total_vol):.2%}'
        unmapped = pd.DataFrame({'BID':  total_vol  - group_broker.sum()[0]}, index=['Unmapped Brokers'])
        group_broker = pd.concat([group_broker, unmapped])
        column0 = group_broker['BID']
        index = group_broker.index

    if col_len == 2:
        fig, axes = plt.subplots(figsize=(10,15), facecolor=facecolor, ncols=2, sharey=True)
        fig.tight_layout()
        axes[0].barh(index, column1, align='center', color=color_blue, alpha=0.8, zorder=10)
        axes[0].set_title(title1, fontsize=18, pad=15, color=color_blue, alpha=0.8, **hfont)
        axes[1].barh(index, column0, align='center', color=color_red, alpha=0.8, zorder=10)
        axes[1].set_title(title0, fontsize=18, pad=15, color=color_red, alpha=0.8, **hfont)

        current_value_1 = axes[0].get_xticks()
        current_value_2 = axes[1].get_xticks()
        axes[0].set_xticklabels([f'{formatted_int(x)}' for x in current_value_1])
        axes[1].set_xticklabels([f'{formatted_int(x)}' for x in current_value_2])

        for i, v in enumerate(column0):
            if v < current_value_2[-1]*0.05:
                axes[1].text(current_value_2[-1]*0.05, i, str(formatted_int(v)), color='black', ha='left', va='center', zorder=11)
            else:
                axes[1].text(v/2, i, str(formatted_int(v)), color='black', ha='left', va='center', zorder=11)
        for i, v in enumerate(column1):
            if v < current_value_1[-1]*0.05:
                axes[0].text(current_value_1[-1]*0.05, i, str(formatted_int(v)), color='black', ha='right', va='center', zorder=11)
            else:
                axes[0].text(v/2, i, str(formatted_int(v)), color='black', ha='right', va='center', zorder=11)

        axes[0].invert_xaxis()
        plt.gca().invert_yaxis()
        axes[0].set(yticks=group_broker.index, yticklabels=group_broker.index)
        axes[0].yaxis.tick_left()
        axes[0].tick_params(axis='y', colors='black') # tick color
    elif col_len == 1:
        fig, axes = plt.subplots(figsize=(10,15), facecolor=facecolor, ncols=1)
        fig.tight_layout()
        if group_broker.columns[0] == "ASK":
            axes.barh(index, column0, align='center', color=color_blue, alpha=0.8, zorder=10)
            axes.set_title(title0, fontsize=18, pad=15, color=color_blue, alpha=0.8, **hfont)
        if group_broker.columns[0] == "BID":
            axes.barh(index, column0, align='center', color=color_red, alpha=0.8, zorder=10)
            axes.set_title(title0, fontsize=18, pad=15, color=color_red, alpha=0.8, **hfont)

        current_value = axes.get_xticks()
        axes.set_xticklabels([f'{formatted_int(x)}' for x in current_value])

        for i, v in enumerate(column0): # TODO: descibptions
            if v < current_value[-1]*0.05:
                axes.text(current_value[-1]*0.05, i, str(formatted_int(v)), color='black', ha='left', va='center', zorder=11)
            else:
                axes.text(v/2, i, str(formatted_int(v)), color='black', ha='left', va='center', zorder=11)

        plt.gca().invert_yaxis()
        axes.set(yticks=group_broker.index, yticklabels=group_broker.index)
        axes.yaxis.tick_left()
        axes.tick_params(axis='y', colors='black') # tick color

    return fig
