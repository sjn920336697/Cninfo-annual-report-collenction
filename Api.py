from flask import Flask, jsonify, abort
from flask import make_response,request
from AR_PDF_PROCESS import Process_file
import time
app = Flask(__name__)

tasks = [
    {
        'id': 1,
        'title': u'Buy groceries',
        'description': u'Milk, Cheese, Pizza, Fruit, Tylenol',
        'done': False
    },
    {
        'id': 2,
        'title': u'Learn Python',
        'description': u'Need to find a good Python tutorial on the web',
        'done': False
    }
]

@app.route('/')
def index():
	return 'shijiannan'

#http://localhost:5000/sjn/api/v1.0/tasks
#get 方法打开等于下载
@app.route('/sjn/api/v1.0/tasks',methods=['GET'])
def get_tesk():
	#return {'task':tasks} #must be str,tuple,json
	#return jsonify({'task':tasks})
	tmp = Process_file()
	return jsonify({'task':tmp})


#http://localhost:5000/sjn/api/v1.0/tasks/1
@app.route('/sjn/api/v1.0/tasks/<int:task_id>',methods=['GET'])
def get_task(task_id):
	task = list(filter(lambda t : t['id'] == task_id, tasks))
	if len(task) == 0:
		abort(404)
	#tmp = 'Process_file()'
	#return jsonify({'task':task[0]})
	#return jsonify({'task':tmp})

@app.errorhandler(404)
def error_hd(error):
	return make_response(jsonify({'error':'Not Found'}),404)

@app.route('/sjn/api/v1.0/tasks',methods=['POST'])
def creat_task():
	if not request.json or not 'title' in request.json:
		abort(400)
	task = {
		'id':tasks[-1]['id']+1 ,
		'title':request.json['title'] ,
		'description':request.json.get('description',''),
		'done':False
	}
	tasks.append(task)
	return jsonify({'task':task}),201 #状态201

if __name__ == '__main__':
	app.run(debug = True)