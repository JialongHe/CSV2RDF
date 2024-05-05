import fs from 'fs';
import path from 'path';
import csv from 'csv-parser';
import rdf from 'rdf-ext';
import { NamedNode, Literal } from 'rdf-data-factory';
import { Writer } from 'n3';

// Base URI and ontology namespaces
const BASE_URI = 'http://assignment3.org/';
const ns = (suffix) => new NamedNode(BASE_URI + suffix);

// Create an RDF dataset to store RDF triples
const graph = rdf.dataset();
const directory = '.'; // Current directory to look up csv files
const csvFiles = fs.readdirSync(directory).filter((file) => file.endsWith('.csv'));

// Transform a CSV file and create RDF triples
const processCSV = (filePath) => {
  return new Promise((resolve, reject) => {
    const fileName = path.basename(filePath, '.csv');

    fs.createReadStream(filePath)
      .pipe(csv()) // Parse CSV
      .on('data', (row) => {
        switch (fileName) {
          case 'Assigned_Hours': {
            const courseCode = row['Course code'];
            const studyPeriod = row['Study Period'];
            const academicYear = row['Academic Year'];
            const teacherId = row['Teacher Id'];
            const hours = row['Hours'];
            const courseInstance = row['Course Instance'];

            // const courseUri = ns(`Course/${courseCode}`);
            // const teacherUri = ns(`Teacher/${teacherId}`);
            const courseInstanceUri = ns(`CourseInstance/${courseInstance}`);
            const assignedHourUri = ns(`AssignedHour/${courseInstance}-${teacherId}`);

            graph.add(rdf.quad(assignedHourUri, ns('rdf:type'), ns('AssignedHour')));
            graph.add(rdf.quad(assignedHourUri, ns('courseCode'), new Literal(courseCode)));
            graph.add(rdf.quad(assignedHourUri, ns('studyPeriod'), new Literal(studyPeriod)));
            graph.add(rdf.quad(assignedHourUri, ns('academicYear'), new Literal(academicYear)));
            graph.add(rdf.quad(assignedHourUri, ns('teacherId'), new Literal(teacherId)));
            graph.add(rdf.quad(assignedHourUri, ns('hours'), new Literal(hours)));
            graph.add(rdf.quad(assignedHourUri, ns('courseInstance'), new Literal(courseInstance)));
            graph.add(rdf.quad(assignedHourUri, ns('AssignedTo'), courseInstanceUri));
            break;
          }

          case 'Course_Instances': {
            const courseCode = row['Course code'];
            const studyPeriod = row['Study period'];
            const academicYear = row['Academic year'];
            const instanceId = row['Instance_id'];
            const examiner = row['Examiner'];

            const courseUri = ns(`Course/${courseCode}`);
            const instanceUri = ns(`CourseInstance/${instanceId}`);
            const examinerUri = ns(`SeniorTeacher/${examiner}`);

            graph.add(rdf.quad(instanceUri, ns('rdf:type'), ns('CourseInstance')));
            graph.add(rdf.quad(instanceUri, ns('instanceId'), new Literal(instanceId)));
            graph.add(rdf.quad(instanceUri, ns('courseCode'), new Literal(courseCode)));
            graph.add(rdf.quad(instanceUri, ns('studyPeriod'), new Literal(studyPeriod)));
            graph.add(rdf.quad(instanceUri, ns('academicYear'), new Literal(academicYear)));
            graph.add(rdf.quad(instanceUri, ns('examiner'), new Literal(examiner)));
            graph.add(rdf.quad(instanceUri, ns('offers'), courseUri));
            graph.add(rdf.quad(examinerUri, ns('examiner'), instanceUri));
            break;
          }

          case 'Programmes': {
            const programmeCode = row['Programme code'];
            const programmeName = row['Programme name'];
            const departmentName = row['Department name'];
            const director = row['Director'];

            const programmeUri = ns(`Programme/${programmeCode}`);
            const directorUri = ns(`SeniorTeacher/${director}`);

            graph.add(rdf.quad(programmeUri, ns('rdf:type'), ns('Programme')));
            graph.add(rdf.quad(programmeUri, ns('programmeName'), new Literal(programmeName)));
            graph.add(rdf.quad(programmeUri, ns('programmeCode'), new Literal(programmeCode)));
            graph.add(rdf.quad(programmeUri, ns('dept'), new Literal(departmentName)));
            graph.add(rdf.quad(programmeUri, ns('director'), new Literal(director)));
            graph.add(rdf.quad(directorUri, ns('directs'), programmeUri));
            break;
          }

          case 'Courses': {
            const courseCode = row['Course code'];
            const courseName = row['Course name'];
            const credits = row['Credits'];
            const level = row['Level'];
            const department = row['Department'];
            const division = row['Division'];
            const ownedBy = row['Owned By'];

            const courseUri = ns(`Course/${courseCode}`);
            const programmeUri = ns(`Programme/${ownedBy}`);

            graph.add(rdf.quad(courseUri, ns('rdf:type'), ns('Course')));
            graph.add(rdf.quad(courseUri, ns('courseCode'), new Literal(courseCode)));
            graph.add(rdf.quad(courseUri, ns('courseName'), new Literal(courseName)));
            graph.add(rdf.quad(courseUri, ns('credits'), new Literal(credits)));
            graph.add(rdf.quad(courseUri, ns('level'), new Literal(level)));
            graph.add(rdf.quad(courseUri, ns('division'), new Literal(division)));
            graph.add(rdf.quad(courseUri, ns('dept'), new Literal(department)));
            graph.add(rdf.quad(courseUri, ns('ownedBy'), new Literal(ownedBy)));
            graph.add(rdf.quad(courseUri, ns('OwnedBy'), programmeUri));
            break;
          }

          case 'Course_plannings': {
            const course = row['Course'];
            const plannedNumberOfStudents = row['Planned number of Students'];
            const seniorHours = row['Senior Hours'];
            const assistantHours = row['Assistant Hours'];

            const courseUri = ns(`Course/${course}`);
            const coursePlanningUri = ns(`CoursePlanning/${course}`);

            graph.add(rdf.quad(coursePlanningUri, ns('rdf:type'), ns('CoursePlanning')));
            graph.add(rdf.quad(coursePlanningUri, ns('course'), new Literal(course)));
            graph.add(rdf.quad(coursePlanningUri, ns('plannedNumberOfStudents'), new Literal(plannedNumberOfStudents)));
            graph.add(rdf.quad(coursePlanningUri, ns('seniorHours'), new Literal(seniorHours)));
            graph.add(rdf.quad(coursePlanningUri, ns('assistantHours'), new Literal(assistantHours)));
            graph.add(rdf.quad(coursePlanningUri, ns('MapsTo'), courseUri));
            break;
          }

          case 'Programme_Courses': {
            const programmeCode = row['Programme code'];
            const studyYear = row['Study Year'];
            const academicYear = row['Academic Year'];
            const course = row['Course'];
            const courseType = row['Course Type'];

            const programmeUri = ns(`Programme/${programmeCode}`);
            const courseUri = ns(`Course/${course}`);
            const programmeCourseUri = ns(`programmeCourses/${programmeCode}-${course}`);

            graph.add(rdf.quad(programmeCourseUri, ns('rdf:type'), ns('ProgrammeCourse')));
            graph.add(rdf.quad(programmeCourseUri, ns('programmeCode'), new Literal(programmeCode)));
            graph.add(rdf.quad(programmeCourseUri, ns('studyYear'), new Literal(studyYear)));
            graph.add(rdf.quad(programmeCourseUri, ns('academicYear'), new Literal(academicYear)));
            graph.add(rdf.quad(programmeCourseUri, ns('course'), new Literal(course)));
            graph.add(rdf.quad(programmeCourseUri, ns('courseType'), new Literal(courseType)));
            graph.add(rdf.quad(programmeCourseUri, ns('BelongsTo'), programmeUri));
            graph.add(rdf.quad(programmeCourseUri, ns('Encompasses'), courseUri));
            break;
          }

          case 'Registrations': {
            const courseInstance = row['Course Instance'];
            const studentId = row['Student id'];
            const status = row['Status'];
            const grade = row['Grade'];

            const registrationUri = ns(`Registration/${courseInstance}-${studentId}`);
            const courseInstanceUri = ns(`CourseInstance/${courseInstance}`);
            const studentUri = ns(`Student/${studentId}`);

            graph.add(rdf.quad(registrationUri, ns('rdf:type'), ns('Registration')));
            graph.add(rdf.quad(registrationUri, ns('courseInstance'), new Literal(courseInstance)));
            graph.add(rdf.quad(registrationUri, ns('studentId'), new Literal(studentId)));
            graph.add(rdf.quad(registrationUri, ns('status'), new Literal(status)));
            graph.add(rdf.quad(registrationUri, ns('grade'), new Literal(grade)));
            graph.add(rdf.quad(registrationUri, ns('EnrolledIn'), studentUri));
            graph.add(rdf.quad(registrationUri, ns('RegistersFor'), courseInstanceUri));
            break;
          }

          case 'Students': {
            const studentId = row['Student id'];
            const studentName = row['Student name'];
            const programme = row['Programme'];
            const year = row['Year'];
            const graduated = row['Graduated'];

            const studentUri = ns(`Student/${studentId}`);

            graph.add(rdf.quad(studentUri, ns('rdf:type'), ns('Student')));
            graph.add(rdf.quad(studentUri, ns('studentId'), new Literal(studentId)));
            graph.add(rdf.quad(studentUri, ns('name'), new Literal(studentName)));
            graph.add(rdf.quad(studentUri, ns('programme'), new Literal(programme)));
            graph.add(rdf.quad(studentUri, ns('year'), new Literal(year)));
            graph.add(rdf.quad(studentUri, ns('graduated'), new Literal(graduated)));
            break;
          }

          case 'Reported_Hours': {
            const courseCode = row['Course code'];
            const teacherId = row['Teacher Id'];
            const hours = row['Hours'];

            const teacherUri = ns(`Teacher/${teacherId}`);
            const courseUri = ns(`CourseInstance/${courseCode}`);
            const reportedHoursUri = ns(`ReportedHour/${courseCode}-${teacherId}`);
            
            graph.add(rdf.quad(reportedHoursUri, ns('rdf:type'), ns('ReportedHours')));
            graph.add(rdf.quad(reportedHoursUri, ns('courseCode'), new Literal(courseCode)));
            graph.add(rdf.quad(reportedHoursUri, ns('teacherId'), new Literal(teacherId)));
            graph.add(rdf.quad(reportedHoursUri, ns('hours'), new Literal(hours)));
            graph.add(rdf.quad(reportedHoursUri, ns('reportedFor'), courseUri));
            graph.add(rdf.quad(reportedHoursUri, ns('reports'), teacherUri));
            break;
          }

          case 'Teaching_Assistants': {
            const teacherId = row['Teacher id'];
            const teacherName = row['Teacher name'];
            const dept = row['Department name'];
            const division = row['Division name'];

            const teacherUri = ns(`TeacherAssistant/${teacherId}`);

            graph.add(rdf.quad(teacherUri, ns('rdf:type'), ns('TeacherAssistant')));
            graph.add(rdf.quad(teacherUri, ns('rdfs:subClassOf'), ns('Teacher')));
            graph.add(rdf.quad(teacherUri, ns('teacherId'), new Literal(teacherId)));
            graph.add(rdf.quad(teacherUri, ns('name'), new Literal(teacherName)));
            graph.add(rdf.quad(teacherUri, ns('dept'), new Literal(dept)));
            graph.add(rdf.quad(teacherUri, ns('division'), new Literal(division)));
            break;
          }

          case 'Senior_Teachers': {
            const teacherId = row['Teacher id'];
            const teacherName = row['Teacher name'];
            const dept = row['Department name'];
            const division = row['Division name'];

            const teacherUri = ns(`SeniorTeacher/${teacherId}`);

            graph.add(rdf.quad(teacherUri, ns('rdf:type'), ns('SeniorTeacher')));
            graph.add(rdf.quad(teacherUri, ns('rdfs:subClassOf'), ns('Teacher')));
            graph.add(rdf.quad(teacherUri, ns('teacherId'), new Literal(teacherId)));
            graph.add(rdf.quad(teacherUri, ns('name'), new Literal(teacherName)));
            graph.add(rdf.quad(teacherUri, ns('dept'), new Literal(dept)));
            graph.add(rdf.quad(teacherUri, ns('division'), new Literal(division)));
            break;
          }
        }
      })
      .on('end', () => {
        console.log(`Finished ${filePath}`);
        // Resolve the promise to indicate completion
        resolve();
      })
      .on('error', (err) => {
        reject(err);
      });
  });
};

const promises = csvFiles.map((file) => {
  const filePath = path.join(directory, file);
  return processCSV(filePath);
});

Promise.all(promises)
  .then(() => {
    console.log(`Graph size of all CSV files: ${graph.size}`);
    const writer = new Writer({ format: 'Turtle' });
    const quads = [];
    graph.forEach((quad) => {
      quads.push(quad);
    });
    writer.addQuads(quads);
    writer.end((error, result) => {
      if (error) {
        console.error('Error during RDF serialization:', error);
      } else {
        const outputPath = path.join(directory, 'rdf.ttl');
        fs.writeFileSync(outputPath, result);
        console.log('RDF data written to', outputPath);
      }
    });
  })
  .catch((error) => {
    console.error('Error processing CSV files:', error);
});
