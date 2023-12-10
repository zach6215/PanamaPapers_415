import tkinter as tk
from tkinter import messagebox
from sparkFunctions import *


def run_time_active():
    timeActive()
    # call time_active


def run_shortest_path():
    try:
        newShortestPath(box_shortest_pathA.get(), box_shortest_pathB.get())
    except:
        print("Invalid input in newShortestPath() or value not found")
        messagebox.showwarning("Unknown Value Or No Path", "Invalid input in newShortestPath() or path not found")
    # call shortest_path


def run_countries_E():
    countries(1)
    # call countries

def run_countries_I():
    countries(2)
    # call countries

def run_countries_O():
    countries(3)
    # call countries

def run_sparkConnected():
    try:
        sparkConnected(box_connected.get())
    except:
        print("Invalid input in sparkConnected() or value not found")
        messagebox.showwarning("Unknown Value", "Invalid input in sparkConnected() or value not found")
    # call function


root = tk.Tk()
root.title("Data Analysis GUI")

btn_time_active = tk.Button(root, text="Run Time Active", command=run_time_active)
btn_shortest_path = tk.Button(root, text="Run Shortest Path", command=run_shortest_path)
btn_countries_E = tk.Button(root, text="Run Entity Country List", command=run_countries_E)
btn_countries_I = tk.Button(root, text="Run Intermediary Country List", command=run_countries_I)
btn_countries_O = tk.Button(root, text="Run Officer Country List", command=run_countries_O)
btn_connected = tk.Button(root, text="Run Connected", command=run_sparkConnected)

btn_time_active.grid(column=0, row=2)
btn_shortest_path.grid(column=1, row=2)
btn_countries_E.grid(column=2, row=0)
btn_countries_I.grid(column=2, row=1)
btn_countries_O.grid(column=2, row=2)
btn_connected.grid(column=3, row=2)

box_shortest_pathA = tk.Entry(root)
box_shortest_pathB = tk.Entry(root)
box_connected = tk.Entry(root)

box_shortest_pathA.grid(column=1, row=0)
box_shortest_pathB.grid(column=1, row=1)
box_connected.grid(column=3, row=0)

root.mainloop()
spark.stop()
