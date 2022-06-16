difference_locations = np.where(df1 != df)
changed_from = df1.values[difference_locations]
changed_to = df.values[difference_locations]
dff = DataFrame({'from': changed_from, 'to': changed_to})
