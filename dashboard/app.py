
        return render.DataGrid(query_by_origin_func(TYPE_CREATOR, "creator"), filters=False)


app = App(app_ui, server)