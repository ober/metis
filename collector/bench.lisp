(load "collector/load.lisp")
(in-package :ctcl)

(defun do-bench ()
  (cloudtrail-report-async "1" "~/test-ct/"))

(defun time-bench ()
  (time (cloudtrail-report-sync "~/test-ct/")))

(defun run-bench () 
  ;;(declare (optimize (safety 3) (speed 0) (debug 3)))
  ;;(defvar *q* (make-instance 'queue))
  ;;(load "collector/load.lisp")
  ;;(princ "XXX: Ensuring connections")
  ;;(db-ensure-connection "metis-test")
  ;;(princ "XXX: Dropping tables")
  ;;(db-create-tables)
  (princ "XXX: Running Test")
  #+sbcl
  (time (cloudtrail-report-async "1" "~/test-ct/"))
  ;; (progn
  ;; 	(sb-sprof:with-profiling (:report :flat) (do-bench)))
  #+lispworks  (hcl:extended-time (cloudtrail-report-async "100" "~/test-ct/"))
  ;;  (progn
  ;;    (hcl:set-up-profiler :package '(ctcl))
  ;;    (hcl:profile (cloudtrail-report-async "10" "~/test-ct/")))
  #+allegro
  (progn
    (setf excl:*tenured-bytes-limit* 524288000)
    (prof::with-profiling (:type :space) (time (cloudtrail-report-async "10" "~/test-ct/")))
    (prof::show-flat-profile))
  #+(or clozure abcl ecl) (progn (time (cloudtrail-report-async "10" "~/test-ct/")))
  
  ;;(cl-store:store *q* "~/q.store")
  ;;(format t "results: size:~A" (queue-length *q*))
  )

(run-bench)
