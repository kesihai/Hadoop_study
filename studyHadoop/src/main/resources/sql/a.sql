select
    a.student_id,
    a.student_name,
    b.subject_name,
    count(e.subject_name) as attend
from students a join subjects b
left join examinations e
on a.student_id = e.student_id
and b.subject_name = e.subject_name
group by a.student_id, b.subject_name