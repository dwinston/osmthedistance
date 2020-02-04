import matplotlib.pyplot as plt


def plot_route(rg, route):
    points = [rg.lat_lon(n) for n in route.nodes]
    lats, lngs = zip(*points)
    fig, ax = plt.subplots()
    ax.plot(lngs, lats)
    for i, (lng, lat) in enumerate(zip(lngs, lats)):
        ax.annotate(i, (lng, lat))
    return fig
