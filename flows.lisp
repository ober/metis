(in-package :metis)

(defun flows-have-we-seen-this-file (file)
  ;;(format t ".")
  (let ((them (load-file-values "flow_files")))
    (if (gethash (file-namestring file) flow-files)
  	t
	nil)))

(defun vpc-flows-report-async (workers path)
  (let ((workers (parse-integer workers))
	(vpc-flows-reports (or path "~/vpc")))


       ))

(defun async-vf-file (x)
  (push (pcall:pexec
	  (funcall #'process-vf-file x)) *mytasks*))

(defun process-vf-file (x)
  (when (equal (pathname-type x) "gz")
    (unless (have-we-seen-this-file x)
      (db-mark-file-processed x)
      ;;(format t "n")
      ;;(format t "New:~A~%" (file-namestring x))
      (parse-vf-contents x))))

(defun parse-vf-contents (x)
  (format t "+")
  (let* ((records (cdr (elt (read-gzip-file x) 0)))
	 (format t "~A" records))))

(defun read-gzip-file (file)
  (uiop:run-program
   (format nil "zcat ~A" file)
   :output :string))

(defun process-vf-file (file)
  (mapcar #'process-vf-line
	  (split-sequence:split-sequence #\linefeed
					 (uiop:run-program (format nil "zcat ~A" file) :output :string))))


(defun process-vf-line (line)
  )

	 ;; 	 (num (length records))
    ;; 	 (btime (get-internal-real-time)))
    ;; ;;(format t "wtf: records:~A~%" (length records))
    ;; (dolist (x records)
    ;;   (let* ((event-time (cdr-assoc :EVENT-TIME x))
    ;; 	     ;;(user-identity (cdr-assoc :ACCESS-KEY-ID (cdr-assoc :USER-IDENTITY x)))
    ;; 	     (event-name (cdr-assoc :EVENT-NAME x))
    ;; 	     (user-agent (cdr-assoc :USER-AGENT x))
    ;; 	     (ip (cdr-assoc :SOURCE-+IP+-ADDRESS x))
    ;; 	     (hostname (get-hostname-by-ip ip))
    ;; 	     (user-identity (cdr-assoc :USER-IDENTITY x))
    ;; 	     (user-name (cdr-assoc :USER-NAME user-identity))
    ;; 	     (user-key (cdr-assoc :ACCESS-KEY-ID user-identity)))
    ;; 	(normalize-insert event-time user-name user-key event-name user-agent (or hostname ip))))
    ;; (let* ((etime (get-internal-real-time))
    ;; 	   (delta (/ (float (- etime btime)) (float internal-time-units-per-second))))
    ;;   (if (and (> delta 0) (> num 99))
    ;; 	  (let ((rps (/ (float num) (float delta))))
    ;; 	    (format t "~%rps:~A rows:~A delta:~A" rps num delta))))))
