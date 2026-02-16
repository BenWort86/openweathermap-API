# --- Imports ---
import pandas as pd
import sqlalchemy
import weather_data_config as cfg
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.dates as mdates
import seaborn as sns
import matplotlib.gridspec as gridspec
from matplotlib.offsetbox import TextArea, VPacker, AnchoredOffsetbox
from scipy import stats as st


# Load forecast vs. actual weather data from database
def connect_to_db():
    '''Create SQLAlchemy engine from config.'''
    return sqlalchemy.create_engine(cfg.Config_Database.database_url())


engine = connect_to_db()

df = pd.read_sql(
    'SELECT * FROM weather_data_forecast_vs_actual',
    con=engine
)


# ------------------------- Plots ------------------------------------------- #

# Plot forecast and actual temperatures including tolerance band
# Grey area indicates period with interrupted data collection
def plot_forecast_vs_actual(ax, d, days):

    F = d.temperature_f
    A = d.temperature_a
    # The tolerance value T changes with horizon; here we take the first value
    T = d.tolerance_in_degree.iloc[0]
    # Lower and Upper tolerance bands
    UB = d.upper_bound_f
    LB = d.lower_bound_f

    # Plot forecast tolerance interval (upper/lower bound)
    ax.fill_between(d.date_f, UB, LB, alpha=0.15, color='blue')
    ax.plot(d.date_f, F, '-', color='blue',
            linewidth=1.5, label='Forecast')
    ax.plot(d.date_f, A, '-', color='red', linewidth=1.5, label='Actual')
    ax.set_title(f'Horizon {days} days | Tolerance +/-{T}\u00B0C')

    start = pd.Timestamp('2026-02-04 10:00', tz='UTC')
    end = pd.Timestamp('2026-02-12 07:00', tz='UTC')

    # Highlight period with interrupted data collection
    ax.axvspan(start, end, alpha=0.2, color='grey')

    handles, labels = ax.get_legend_handles_labels()

    return handles, labels


# Plot cumulative mean error (ME) and instantaneous error (E) over time
# Grey shaded area highlights system downtime (data missing)
def plot_error(ax, d, days):

    ME = d.cum_bias
    E = d.error_val

    ax.plot(d.date_f, ME, label='Mean Error',
            color='red', linewidth=2.0, linestyle='-')
    ax.plot(d.date_f, E, label='Error',
            color='blue', linewidth=1.2, alpha=0.7, linestyle='-')
    ax.set_title(f'Horizon {days} days')

    start = pd.Timestamp('2026-02-04 10:00', tz='UTC')
    end = pd.Timestamp('2026-02-12 07:00', tz='UTC')

    # Highlight period with interrupted data collection
    ax.axvspan(start, end, alpha=0.2, color='grey')

    handles, labels = ax.get_legend_handles_labels()

    return handles, labels


# Plot cumulative hit rate within defined tolerance
# Values capped at < 100% to avoid distortions
def plot_hit_rate(ax, d, days):

    # The tolerance value T changes with horizon; here we take the first value
    T = d.tolerance_in_degree.iloc[0]
    HR = d.cum_hit_rate

    ax.plot(d.date_f, HR, label='Hit Rate',
            color='red', linewidth=1, alpha=0.8)
    ax.set_title(f'Horizon {days} days | Tolerance +/-{T}\u00B0C')

    start = pd.Timestamp('2026-02-04 10:00', tz='UTC')
    end = pd.Timestamp('2026-02-12 07:00', tz='UTC')

    # Highlight period with interrupted data collection
    ax.axvspan(start, end, alpha=0.2, color='grey')

    # No legend is returned because information is already in the title
    return [], []


# Plot cumulative weighted MAPE over time
def plot_wmape(ax, d, days):

    WMAPE = d.cum_wmape

    ax.plot(d.date_f, WMAPE, label='wMAPE',
            color='black', linewidth=1, alpha=0.8)
    ax.set_title(f'Horizon {days} days')

    start = pd.Timestamp('2026-02-04 10:00', tz='UTC')
    end = pd.Timestamp('2026-02-12 07:00', tz='UTC')

    # Highlight period with interrupted data collection
    ax.axvspan(start, end, alpha=0.2, color='grey')

    # No legend is returned because information is already in the title
    return [], []


# Plot cumulative MAE (Mean Absolute Error) and RMSE (Root Mean Squared Error)
# on the left axis.
# Plot the MAE/RMSE ratio on a secondary right axis to assess relative error
# dispersion over time.
# Grey shaded area highlights system downtime
def plot_mae_rmse(ax, d, days):

    # values
    MAE = d.cum_mae
    RMSE = d.cum_rmse
    R = MAE/RMSE

    # --- plot mae & rmse ---
    ax.set_title(f'Horizon {days} days')

    # mae
    ax.plot(d.date_f, MAE, label='MAE',
            color='blue', linewidth=2, alpha=0.8)

    # rmse
    ax.plot(d.date_f, RMSE, label='RMSE',
            color='#5fa2e0', linewidth=1.5, alpha=0.8, linestyle='--')

    # label & axes
    ax.set_ylabel('°C', fontsize=10, color='blue')
    ax.tick_params(axis='y', labelcolor='blue')

    start = pd.Timestamp('2026-02-04 10:00', tz='UTC')
    end = pd.Timestamp('2026-02-12 07:00', tz='UTC')

    # Highlight period with interrupted data collection
    ax.axvspan(start, end, alpha=0.2, color='grey')

    ax2 = ax.twinx()

    # --- plot mae/rmse ---
    ax2.plot(d.date_f, R, label='MAE/RMSE',
             color='orange', linewidth=1, alpha=0.8, linestyle='--')

    ax2.tick_params(axis='y', labelcolor='orange')

    # Disable grid on secondary axis to avoid overlapping grid lines
    ax2.grid(False)

    handles1, labels1 = ax.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()

    return handles1 + handles2, labels1 + labels2


# Scatterplot of forecast vs. actual temperatures per horizon
# Highlights over- and under-forecasted points with color coding
# Plots line of identity (LoI) and linear regression fit for reference
# Displays:
#   - Percentage of over- and underforecast points
#   - Regression equation
#   - Correlation coefficient
def plot_scatter(ax, d, days):

    x = d.temperature_f
    y = d.temperature_a

    # Identify over- and under-forecast points
    over = d[y > x]
    under = d[y < x]

    lg = len(over) + len(under)

    # Compute percentages for annotations
    p_over = len(over)/lg * 100
    p_under = len(under)/lg * 100

    # Compute correlation coefficient
    corr = x.corr(y)

    # Plot scatter points - OVERFORECAST -
    ax.scatter(over.temperature_f, over.temperature_a, alpha=0.8,
               color=np.where(over.temperature_a >
                              over.temperature_f, '#e41a1c', '#999999'),
               s=20,
               zorder=1,
               label='Overforecast')

    # Plot scatter points - UNDERFORECAST -
    ax.scatter(under.temperature_f, under.temperature_a, alpha=0.8,
               color=np.where(under.temperature_a <
                              under.temperature_f, '#377eb8', '#999999'),
               s=20,
               zorder=1,
               label='Underforcast')

    # Linear regression fit# Linear regression fit
    idx = np.isfinite(x) & np.isfinite(y)
    b, a = np.polyfit(x[idx], y[idx], deg=1)

    # Line of identity
    lims = [
        min(x.min(), y.min()),
        max(x.max(), y.max())
    ]

    ax.plot(lims, lims, 'k--', alpha=0.8, label='LoI')

    # Regression line
    ax.plot(x, a + b*x, 'k', lw=0.6, label='$\\hat{{actual}}$')
    ax.set_title(f'Horizon {days} days')

    # Annotate over/underforecast percentages
    pta1 = TextArea(f'{p_over:.2f}%', textprops=dict(
        color='#e41a1c', fontsize=10))
    pta2 = TextArea(f'{p_under:.2f}%', textprops=dict(
        color='#377eb8', fontsize=10))

    vp = VPacker(children=[pta1, pta2], align='left', pad=0, sep=2)

    pbox = AnchoredOffsetbox(loc=2, child=vp, frameon=True, pad=0.3,
                             bbox_to_anchor=(0.02, 0.97),
                             bbox_transform=ax.transAxes,
                             borderpad=0.3)

    ax.add_artist(pbox)

    # Annotate regression equation# Annotate regression equation
    rta = TextArea(f'$\\hat{{actual}} = {b:.2f} \\cdot forecast + {a:.2f}$',
                   textprops=dict(color='black', fontsize=10))

    vp = VPacker(children=[rta], align='left', pad=0, sep=2)

    rbox = AnchoredOffsetbox(loc=2, child=vp, frameon=True, pad=0.3,
                             bbox_to_anchor=(0.38, 0.18),
                             bbox_transform=ax.transAxes,
                             borderpad=0.3)

    ax.add_artist(rbox)

    # Annotate correlation coefficient
    cta = TextArea(f'${{corr}} = {corr:.2f}$', textprops=dict(
        color='black', fontsize=10))

    vp = VPacker(children=[cta], align='left', pad=0, sep=2)

    cbox = AnchoredOffsetbox(loc=2, child=vp, frameon=True, pad=0.3,
                             bbox_to_anchor=(0.738, 0.33),
                             bbox_transform=ax.transAxes,
                             borderpad=0.3)

    ax.add_artist(cbox)

    # Enable grid
    ax.grid(True)

    handles, labels = ax.get_legend_handles_labels()

    return handles, labels


# --------------------------------------------------------------------------- #

# Iterate over all plot types and forecast horizons
# For each metric, create a 3x2 subplot grid (one subplot per forecast horizon)
# Shared legend positioned above the subplots
plot_types = {
    'Forecast vs. Actual (°C)': plot_forecast_vs_actual,
    'Error & Mean Error (°C)': plot_error,
    'Hit Rate (%)': plot_hit_rate,
    'wMAPE (%)': plot_wmape,
    'MAE & RMSE': plot_mae_rmse,
    'Scatterplot of Forecast vs. Actual (°C)': plot_scatter
}

# Group data by forecast horizon
groups = list(df.groupby('forecast_horizon_days'))

for title, plot_fn in plot_types.items():

    # Create figure with 3x2 grid
    fig, axs = plt.subplots(3, 2, figsize=(10, 10))

    # Flatten 2D array of axes for easier iteration
    axs = axs.flatten()

    # Loop through each horizon and plot
    for i, (days, d) in enumerate(groups):
        handels, labels = plot_fn(axs[i], d, days)
        axs[i].grid(True)
        # Format x-axis as dates for all plots except scatterplot
        if title != list(plot_types)[5]:
            axs[i].xaxis.set_major_locator(mdates.AutoDateLocator())
            axs[i].xaxis.set_major_formatter(
                mdates.ConciseDateFormatter(mdates.AutoDateLocator())
            )
    # Remove empty subplot if fewer horizons than grid cells
    fig.delaxes(axs[5])

    # Set figure title, appending 'Over Time' for time series plots
    fig.suptitle(
        np.where(title != 'Scatterplot of Forecast vs. Actual (°C)',
                 f'{title} Over Time', f'{title}'),
        fontsize=20,
        y=0.98
    )

    # Adjust spacing
    fig.subplots_adjust(
        left=0.07,
        right=0.90,
        top=0.88,
        bottom=0.07,
        hspace=0.4,
        wspace=0.3
    )

    # Add shared legend above subplots
    fig.legend(
        handels, labels,
        loc='upper center',
        ncol=3,
        frameon=False,
        bbox_to_anchor=(0.5, 0.94)
    )

    # Add axis labels for scatterplot only
    if title == 'Scatterplot of Forecast vs. Actual (°C)':
        fig.supxlabel('Forecast')
        fig.supylabel('Actual')

    plt.show()

# --------------------------------------------------------------------------- #

# Visualize error distribution for each forecast horizon
# Combination of boxplot (top) and histogram (bottom) in a 3x2 grid
fig = plt.figure(figsize=(10, 10))
outer_gs = gridspec.GridSpec(3, 2, hspace=0.4, wspace=0.25)

all_handles = []  # Store legend handles
all_labels = []   # Store legend labels

for i, (days, d) in enumerate(groups):

    E = d.error_val
    B = d.horizon_bias.iloc[0]
    SD = np.std(E)

    row, col = divmod(i, 2)  # Determine row and column in outer grid

    # Create inner grid: top 25% boxplot, bottom 75% histogram
    inner_gs = gridspec.GridSpecFromSubplotSpec(
        2, 1,
        subplot_spec=outer_gs[row, col],
        height_ratios=[0.25, 0.75],
        hspace=0.05
    )

    # Top subplot for boxplot
    ax_box = fig.add_subplot(inner_gs[0])

    # Bottom subplot for histogram
    ax_hist = fig.add_subplot(inner_gs[1], sharex=ax_box)

    # Boxplot of error
    sns.boxplot(x=E, ax=ax_box)

    # Add mean and ±1 standard deviation lines
    line_avg = ax_box.axvline(B, color='red', linestyle='--',
                              linewidth=1.5, alpha=0.6, label='AVG')
    line_std1 = ax_box.axvline(
        B - SD, color='red', linestyle=':', alpha=0.5, label='STD')
    line_std2 = ax_box.axvline(B + SD, color='red', linestyle=':', alpha=0.5)

    # Add legend handles only once
    if i == 0:
        all_handles.extend([line_avg, line_std1])
        all_labels.extend([line_avg.get_label(), line_std1.get_label()])

    ax_box.set(yticks=[])
    ax_box.set_title(f'Horizon {days} days', fontsize=10)

    # Histogram with KDE
    sns.histplot(E, bins=15, kde=True, ax=ax_hist, alpha=0.3)
    ax_hist.axvline(B, color='red', linestyle='--', linewidth=1.5, alpha=0.6)
    ax_hist.axvline(B - SD, color='red', linestyle=':', alpha=0.5)
    ax_hist.axvline(B + SD, color='red', linestyle=':', alpha=0.5)
    ax_hist.set(ylabel=None, xlabel=None)

    # Remove top and left spines for clean look
    sns.despine(ax=ax_box, left=True)
    sns.despine(ax=ax_hist)

# Set overall figure title
fig.suptitle(
    'Error Distribution',
    fontsize=14,
    y=0.98
)

# Adjust spacing between subplots
fig.subplots_adjust(
    left=0.07,
    right=0.90,
    top=0.88,
    bottom=0.07,
    hspace=0.4,
    wspace=0.3
)

# Add shared legend above subplots
fig.legend(
    all_handles, all_labels,
    loc='upper center',
    ncol=3,
    frameon=False,
    bbox_to_anchor=(0.5, 0.94)
)

# Shared axis labels
fig.supxlabel('Error')
fig.supylabel('Frequency')

plt.show()

# --------------------------------------------------------------------------- #

# Compute descriptive statistics of forecast errors for each horizon
# Includes min, 25th percentile, mean, standard deviation, variance, median,
# 75th percentile, max, skewness, and kurtosis
error_dist_stats = []

for days, d in groups:

    e = d.error_val.dropna()

    error_dist_stats.append(
        [
            days,
            np.min(e),
            np.percentile(e, 25),
            np.mean(e),
            np.std(e),
            np.var(e),
            np.median(e),
            np.percentile(e, 75),
            np.max(e),
            st.skew(e),
            st.kurtosis(e)
        ]
    )

    # Convert to DataFrame and sort by forecast horizon
    error_dist_stats_df = pd.DataFrame(
        error_dist_stats,
        columns=['Day',
                 'Min',
                 '25%-Perc.',
                 'Mean',
                 'Std',
                 'Var',
                 'Median',
                 '75%-Perc.',
                 'Max',
                 'Skew',
                 'Kurt'
                 ]
    ).sort_values('Day')


# Extract aggregated performance metrics per horizon
# Compute relative changes (%) between consecutive horizons
# Rename columns for better readability in reports
performance_stats = df[[
    'forecast_horizon_days',
    'horizon_bias',
    'horizon_mae',
    'horizon_rmse',
    'horizon_q_mae_rmse',
    'horizon_wmape',
    'horizon_hit_rate',
    'tolerance_in_degree']].drop_duplicates(keep='first')

# Compute relative percentage change for selected metrics
for i in range(1, 7):
    col = performance_stats.columns[i]
    performance_stats[f'{col}_rel_change'] = (
        performance_stats.iloc[:, i]
        .pct_change()
        * 100
    )

# Rename columns for clarity in Markdown tables
performance_stats = performance_stats.rename(columns={
    'forecast_horizon_days': 'Day',
    'horizon_bias': 'Bias (°C)',
    'horizon_mae': 'MAE (°C)',
    'horizon_rmse': 'RMSE (°C)',
    'horizon_q_mae_rmse': 'MAE/RMSE',
    'horizon_wmape': 'wMAPE (%)',
    'horizon_hit_rate': 'Hit (%)',
    'tolerance_in_degree': 'Tol. (°C) +/-',
    'horizon_bias_rel_change': 'Bias Δ%',
    'horizon_mae_rel_change': 'MAE Δ%',
    'horizon_rmse_rel_change': 'RMSE Δ%',
    'horizon_q_mae_rmse_rel_change': 'MAE/RMSE Δ%',
    'horizon_wmape_rel_change': 'wMAPE Δ%',
    'horizon_hit_rate_rel_change': 'Hit Δ%'
})

# Select columns for final reporting table
performance_stats_table = performance_stats[[
    'Day',
    'Bias (°C)',
    'Bias Δ%',
    'MAE (°C)',
    'MAE Δ%',
    'RMSE (°C)',
    'RMSE Δ%',
    'MAE/RMSE',
    'MAE/RMSE Δ%',
    'wMAPE (%)',
    'wMAPE Δ%',
    'Hit (%)',
    'Hit Δ%',
    'Tol. (°C) +/-'
]]

# Round values for readability
df_rounded_performance_stats = performance_stats_table.round(2)

# Convert to Markdown table for GitHub or report
md_table_perfromance_stats = df_rounded_performance_stats.to_markdown(
    index=False)
print(md_table_perfromance_stats)

# Round values for readability
df_rounded_error_dist_stats = error_dist_stats_df.round(2)

# Convert to Markdown table for GitHub or report
md_table_error_dist_stats = df_rounded_error_dist_stats.to_markdown(
    index=False)
print(md_table_error_dist_stats)

# --------------------------------------------------------------------------- #

# Plot key performance metrics across forecast horizons
# Each subplot shows one metric (Bias, MAE, RMSE, MAE/RMSE, wMAPE, Hit Rate)
# Enables visual comparison of forecast accuracy as horizon increases

# Create figure with 3x2 grid
fig, axs = plt.subplots(3, 2, figsize=(11, 10))

# Flatten 2D array of axes for easier iteration
axs = axs.flatten()

# Forecast horizon in days
x = performance_stats['Day']

# Define colors for each metric
colors = {
    1: '#8B0000',   # Bias
    2: '#1f77b4',   # MAE
    3: '#005ab5',   # RMSE
    4: '#ff7f0e',   # MAE/RMSE
    5: '#2ca02c',   # wMAPE
    6: '#228B22'    # Hit
}

for i in range(1, 7):

    ax = axs[i-1]
    y = performance_stats.iloc[:, i]

    # Plot metric vs. horizon day
    ax.plot(
        x,
        y,
        linewidth=2.5,
        marker='o',
        markersize=6,
        color=colors[i]
    )

    # Title each subplot with metric name
    ax.set_title(
        performance_stats.columns[i],
        fontsize=11,
        fontweight='bold',
        pad=8
    )

    # Add light horizontal gridlines
    ax.grid(axis='y', alpha=0.3)

    # Label y-axis depending on metric type
    if i in (1, 2, 3):
        ax.set_ylabel('°C', fontsize=9)
    elif i in (5, 6):
        ax.set_ylabel('%', fontsize=9)
    else:
        ax.set_ylabel('')

# Add overall figure title
fig.suptitle(
    'Forecast Performance by Horizon',
    fontsize=20,
    y=0.98
)

# Label x-axis for the figure
fig.supxlabel('Horizon Day')

# Adjust subplot spacing and figure margins
fig.subplots_adjust(
    left=0.08,
    right=0.97,
    top=0.92,
    bottom=0.08,
    hspace=0.5,
    wspace=0.25
)

plt.show()
