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
  (destructuring-bind (date version account_id interface_id srcaddr dstaddr srcport dstport protocol packets bytez start end action status)
;2016-07-20T18:04:30.000Z 2 224108527019 eni-0016955d 10.16.3.11 10.16.3.13 8300 46632 6 3984 330672 1469037870 1469038469 ACCEPT OK
      (split-sequence:split-sequence #\Space line)
    (format t "src:~A dst:~A sp:~A dp:~A~%" srcaddr dstaddr srcport dstport)))
