(in-package :metis)

(defun do-bench ()
  (setf *DB* "metis")
  ;;(declare (optimize (safety 3) (speed 0) (debug 3)))
  (defparameter BENCHING t)
  (cloudtrail-report-async "4" "~/test-ct/"))
;;  (exit))

(defun run-bench ()
  (let ((varz '(*files *h* *mytasks* *DB* *pcallers* dbtype *q*)))
    (mapcar #'(lambda (x)
		(setf x nil)) varz))
  ;;(psql-ensure-connection "metis")
  ;;(princ "XXX: Dropping tables")
  (psql-recreate-tables)
  (princ "XXX: Running Test")
  #+sbcl (progn
	   (sb-sprof:with-profiling (:report :flat) (do-bench)))
  #+lispworks (progn
		(hcl:set-up-profiler :package '(metis))
		(hcl:profile (do-bench)))
  ;; #+allegro (progn
  ;; 	       (setf excl:*tenured-bytes-limit* 524288000)
  ;; 	       (prof::with-profiling (:type :space) (metis::do-bench))
  ;; 	       (prof::show-flat-profile))
  #+(or allegro clozure abcl ecl)(time (do-bench)))
