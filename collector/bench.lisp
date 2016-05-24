;;(load "collector/load.lisp")
(in-package :ctcl)

(defun do-bench ()
  (declare (optimize (safety 3) (speed 0) (debug 3)))
  (cloudtrail-report-async "10" "~/test-ct/"))

(defun run-bench () 
  ;;(load "collector/load.lisp")
  ;;(princ "XXX: Ensuring connections")
  (db-ensure-connection "metis-test")
  ;;(princ "XXX: Dropping tables")
  (db-create-tables)
  (princ "XXX: Running Test")
  #+sbcl (time (do-bench))
  
  ;; (progn
  ;; 	(sb-sprof:with-profiling (:report :flat) (do-bench)))
  #+lispworks  (hcl:extended-time (do-bench))
  ;;  (progn
  ;;    (hcl:set-up-profiler :package '(ctcl))
  ;;    (hcl:profile (do-bench))
  #+allegro
  (progn
    (setf excl:*tenured-bytes-limit* 524288000)
    (prof::with-profiling (:type :space) (ctcl::do-bench))
    (prof::show-flat-profile))
  #+(or clozure abcl ecl) (progn (do-bench))

  ;;(format t "results: size:~A" (queue-length *q*))  
  ;;(cl-store:store *q* "~/q.store")
  )

;;(run-bench)
