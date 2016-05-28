(defpackage :metis/bench
  (:use :common-lisp :metis/main :metis/ctcl :common-lisp :fare-memoization :cl-fad :gzip-stream :cl-json)
  (:import-from :yason)
  (:export #:do-bench
	   #:run-bench))


(in-package :metis/bench)

(defun do-bench ()
  (declare (optimize (safety 3) (speed 0) (debug 3)))
  (cloudtrail-report-async "10" "~/test-ct/"))

(defun run-bench () 
  ;;(princ "XXX: Ensuring connections")
  (db-ensure-connection "metis-test")
  ;;(princ "XXX: Dropping tables")
  (db-recreate-tables)
  (princ "XXX: Running Test")
  #+sbcl (time (do-bench))
  
  ;; (progn
  ;; 	(sb-sprof:with-profiling (:report :flat) (do-bench)))
  #+lispworks  (hcl:extended-time (do-bench))
  ;;  (progn
  ;;    (hcl:set-up-profiler :package '(ctcl))
  ;;    (hcl:profile (do-bench))
  #+allegro
  (time (do-bench))
  ;;(progn
    ;;(setf excl:*tenured-bytes-limit* 524288000)
    ;;(prof::with-profiling (:type :space) (ctcl::do-bench))
    ;;(prof::show-flat-profile))
  #+(or clozure abcl ecl) (time (do-bench))

  ;;(format t "results: size:~A" (queue-length *q*))  
  ;;(cl-store:store *q* "~/q.store")
  )

;;(run-bench)
