import json

def build_chart_html(js_code, chart_elements):
  chart_divs = ''.join('<div id="' + str(e) + '"></div>'
                       for e in chart_elements)
  chart_html = '<html><head>'
  chart_html += ('<script type="text/javascript" '
                 'src="https://www.gstatic.com/charts/loader.js">')
  chart_html += '</script><script type="text/javascript">' + js_code + '</script>'
  chart_html += '</head><body>' + chart_divs + '</body></html>'
  return chart_html

def cell_to_js(cell):
  if cell is None:
    return 'null'
  cell_type = type(cell)
  if cell_type is int or cell_type is float:
    return str(cell)
  elif cell_type is bool:
    if cell:
      return 'true'
    else:
      return 'false'
  elif cell_type is str:
    return "'%s'" % cell
  return None

def get_cell_type(cell):
  cell_type = type(cell)
  if cell_type is int or cell_type is float:
    return 'number'
  elif cell_type is bool:
    return 'boolean'
  elif cell_type is str:
    return 'string'
  return None

def build_data_table_js(columns, rows):
  js_code = 'var data = new google.visualization.DataTable();'
  # Add columns
  for c_type, c_name in columns:
    js_code += 'data.addColumn(\'' + c_type + '\',\'' + c_name +'\');'
  # Add rows
  js_code += 'data.addRows(['
  js_code += ','.join('[%s]' % ','.join(cell_to_js(cell) for cell in row)
                      for row in rows)
  js_code += ']);'
  return js_code

def build_chart_js(element_id, chart_type, columns, rows, chart_options=None):
  if chart_options is not None:
    chart_options_json = json.dumps(chart_options)
  else:
    chart_options_json = '{}'
  js_code = 'function draw_' + element_id + '(){'
  js_code += build_data_table_js(columns, rows)
  js_code += ("new google." + chart_type + "("
              "document.getElementById('" + element_id + "'))"
              ".draw(data,JSON.parse('" + chart_options_json + "'));")
  js_code += '}'
  return js_code

def build_chart_page(chart_specs):
  """ Builds the full html/javascript code to display a page of charts

  Args:
    charts_spec: A list of chart specification dictionaries. Each dictionary
    has the following format:

    {'name': <name of the chart>,
     'type': <chart type>,
     'columns': [(type, name)],
     'data': <2D array of cells>,
     'options': <dictionary of google charts API options (optional field)>
    }
  """
  # Load chart packages
  js_code = "google.charts.load('current', {'packages':["
  js_code += "'corechart',"
  js_code += "'line',"
  js_code += "'scatter'"
  js_code += "]});"
  # Create the callback that calls all charts' draw functions
  js_code += "google.charts.setOnLoadCallback(draw_charts_mangle_mangle);"
  js_code += "function draw_charts_mangle_mangle(){"
  for chart in chart_specs:
    js_code += "draw_" + chart['name'] + "();"
  js_code += "}"
  # Create the individual functions to draw each chart
  for chart in chart_specs:
    chart_options = None
    if 'options' in chart:
      chart_options = chart['options']
    js_code += build_chart_js(chart['name'], chart['type'], 
                              chart['columns'], chart['data'], chart_options)
  html_code = build_chart_html(js_code,
                               [chart['name'] for chart in chart_specs])
  return html_code
