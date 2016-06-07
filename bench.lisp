(in-package :metis)

(defun do-bench ()
  (setf debugg t)
  (setf *DB* "metistest")
  ;;(declare (optimize (safety 3) (speed 0) (debug 3)))
  (defparameter BENCHING t)
  (cloudtrail-report-async "8" "~/test-ct/"))


(defun run-bench () 
  (let ((varz '(*files *h* *mytasks* *DB* *pcallers* dbtype *q*)))
    (mapcar #'(lambda (x)
		(setf x nil)) varz))
  (psql-ensure-connection "metistest")
  ;;(princ "XXX: Dropping tables")
  (psql-recreate-tables "metistest")
  (princ "XXX: Running Test")
  ;;#+sbcl (time (do-bench))
  ;; (progn
  ;; 	(sb-sprof:with-profiling (:report :flat) (do-bench)))
  ;;#+lispworks  (hcl:extended-time (do-bench))
  ;;  (progn
  ;;    (hcl:set-up-profiler :package '(ctcl))
  ;;    (hcl:profile (do-bench))
  ;;  #+allegro
  ;;(progn
    ;;(setf excl:*tenured-bytes-limit* 524288000)
    ;;(prof::with-profiling (:type :space) (ctcl::do-bench))
    ;;(prof::show-flat-profile))
  ;;#+(or clozure abcl ecl) (time (do-bench))
  ;;(format t "results: size:~A" (queue-length *q*))  
  ;;(cl-store:store *q* "~/q.store")
  (do-bench)
  )

;;(run-bench)
