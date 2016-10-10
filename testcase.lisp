(load "~/quicklisp/setup.lisp")
(ql:quickload :pcall-queue)

(defun deq (queue)
  (let ((store nil))
    (loop while (not (pcall-queue:queue-empty-p queue))
       do
	 (setf store (pcall-queue:queue-pop queue)))))


(defun enq (queue max-hash-items)
  (loop for y from 0 to max-hash-items
     do (progn
	  (pcall-queue:queue-push "some text for testing some text for testing some text for testing some text for testing"
				  queue))))

(defun test-case ()
  (let ((queue (pcall-queue:make-queue))
	(total-runs 10000)
	(max-hash-items 100000)
	(store nil))
    (loop for x from 0 to total-runs
       do (progn
	    (format t "enqueue ~A~%" x)
	    (enq queue max-hash-items)
	    (format t "dequeue ~A~%" x)
	    (deq queue)))))

(test-case)
