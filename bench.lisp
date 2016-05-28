(in-package :metis)

(defun do-bench ()
  ;;(declare (optimize (safety 3) (speed 0) (debug 3)))
  (cloudtrail-report-async "1" "~/small-ct/"))


(defun run-bench () 
  (defvar *BENCHING* "yes")
  ;;(princ "XXX: Ensuring connections")
  (db-ensure-connection "metis-test")
  ;;(princ "XXX: Dropping tables")
  (db-recreate-tables)
  (princ "XXX: Running Test")
  #+sbcl (time (do-bench))
  ;; (progn
  ;; 	(sb-sprof:with-profiling (:report :flat) (do-bench)))
  #+lispworks
  (progn
    (hcl:set-up-profiler :package '(metis))
    (hcl:profile (do-bench)))
  #+allegro
  (progn
    (prof::with-profiling (:type :time) (do-bench))
    (prof::show-flat-profile))
  #+(or clozure abcl ecl) (time (do-bench))

  ;;(format t "results: size:~A" (queue-length *q*))  
  ;;(cl-store:store *q* "~/q.store")
  )

;;(run-bench)
