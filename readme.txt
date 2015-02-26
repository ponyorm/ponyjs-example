In this example we use a simplified diagram of an university: https://editor.ponyorm.com/user/pony/University. 

In order to check the example you need to launch file index.py using the following command: python index.py. Then open the url http://localhost:5000 in your browser. You need to have Flask and Pony installed. (pip install flask; pip install pony)

In this single page application we get all departments, groups and courses from the server at once. Then you can click any department and see related groups and courses. Once you click to a group or course, a separate request will be sent to the backend which will get all students for the selected group or course. You can modify values of the objects and click Save button. In this case the updated attributes will be sent to the backend and saved in the database.
