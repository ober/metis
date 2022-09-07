;;(ql:quickload :metis)
(in-package :metis)

(defun init-manardb-bench()
  (if (and (eql (hash-table-count *manard-files*) 0) *metis-need-files*)
      (allocate-file-hash)))

(defun do-bench ()
  (defvar *db-backend* :manardb)
  (init-manardb-bench)
  (manardb:use-mmap-dir "~/ct-manardb/")
  (defvar *benching* t)
  ;;(cloudtrail-report-async "1" "~/nov/"))
  (cloudtrail-report-async "4" "~/jan/"))

(defun run-bench ()
  (princ "XXX: Running Test")
  #+sbcl ;;(time (do-bench))
  (progn
    (sb-sprof:with-profiling (:report :flat) (do-bench)))
  #+lispworks  ;;(hcl:extended-time (do-bench))
  (progn
    ;;(hcl:set-up-profiler :package '(metis))
    (hcl:profile (do-bench)))
  #+allegro
  (progn
    (setf excl:*tenured-bytes-limit* 524288000)
    ;;    (setf *maxsamples* 100)
    (prof::with-profiling (:type :time) (metis::do-bench))
    (prof::show-call-graph)
    (prof::show-flat-profile))

  #+(or clozure abcl ecl) (time (do-bench))
  )

;;(run-bench)
