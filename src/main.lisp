(in-package :metis)

(defun argv ()
  (or
   #+sbcl sb-ext:*posix-argv*
   #+clozure (ccl::command-line-arguments)
   #+allegro (sys:command-line-arguments)
   #+lispworks sys:*line-arguments-list*
   nil))

#+sbcl
(defun sbcl-entry()
  (handler-case (main)
    (sb-sys:interactive-interrupt ()
      (uiop:quit))))

(defun usage ()
  (format t "a - cloudtrail-report-async~%")
  (format t "b - bench-vpc-flows-report-sync arg~%")
  (format t "bench - run benchmark~%")
  (format t "lc - get-unique-conversation~%")
  (format t "lec - get-errorcode-list~%")
  (format t "gre - get-response-elements~%")
  (format t "lev - get-event-list~%")
  (format t "lr - get-region-list~%")
  (format t "lip - get-sourceips-list~%")
  (format t "lsed - get-serviceEventDetails~%")
  (format t "lecat - get-eventCategory~%")
  (format t "ln - get-name-list~%")
  (format t "lva - get-vpc-action-list~%")
  (format t "lvd - get-vpc-date-list~%")
  (format t "lvda - get-vpc-dstaddr-list~%")
  (format t "lvdp - get-vpc-srcport-list~%")
  (format t "lvi - get-vpc-interface-id-list~%")
  (format t "lvp - get-vpc-protocols-list~%")
  (format t "lvpc - list-all-vpc~%")
  (format t "lvs - get-vpc-status-list~%")
  (format t "lvsa - get-vpc-srcaddr-list~%")
  (format t "lvsp - get-vpc-srcport-list~%")
  (format t "s - cloudtrail-report-sync arg~%")
  (format t "sec - get-by-errorcode arg~%")
  (format t "seca - ct-get-all-errors~%")
  (format t "sev - get-by-event arg~%")
  (format t "sin - get-useridentity-by-name arg~%")
  (format t "sip - get-by-sourceip arg~%")
  (format t "sn - get-by-name arg~%")
  (format t "sr - get-by-region arg~%")
  (format t "st - get-stats~%")
  (format t "va - vpc-flows-report-async arg dir~%")
  (format t "vs - vpc-flows-report-sync arg~%")
  (format t "vsp - vpc-search-by-srcport port~%")
  (format t "vdp - vpc-search-by-dstport port~%")
  (format t "vsi - vpc-search-by-srcip port~%")
  (format t "vdi - vpc-search-by-dstip port~%")
  (format t "web - launch web interface~%")
  )

(defun process-args (args)
  #+sbcl (setf args (nth 1 args))
  ;;(format t "!! args: ~a type: ~a length: ~a" args (type-of args) (length args))
  (if (< (length args) 1)
      (progn
        (usage)
        (uiop:quit)))
  (let* ((verb (nth 1 args))
         (rest (cdr args)))
    (cond

      ((equal "bench" verb) (run-bench))
      ((equal "cc" verb) (ssdb/count-calls))
      ((equal "gre" verb) (get-response-elements))
      ((equal "gu" verb) (db-get-unique (cadr rest)))
      ((equal "lapi" verb) (get-apiVersion))
      ((equal "index" verb) (db-index (cadr rest)))
      ((equal "lc" verb) (get-unique-conversation))
      ((equal "lconsole" verb) (get-sessionCredentialFromConsole))
      ((equal "lcts" verb) (get-cts))
      ((equal "lec" verb) (db-get-unique-errorcode))
      ((equal "lecat" verb) (get-eventCategory))
      ((equal "lev" verb) (db-get-unique-events))
      ((equal "lip" verb) (get-sourceips-list))
      ((equal "ln" verb) (db-get-unique-names))
      ((equal "lr" verb) (get-region-list))
      ((equal "lsed" verb) (get-serviceEventDetails))
      ((equal "ltls" verb) (get-tlsDetails))
      ((equal "lva" verb) (get-vpc-action-list))
      ((equal "lvcep" verb) (get-vpcEndpointId))
      ((equal "lvd" verb) (get-vpc-date-list))
      ((equal "lvda" verb) (get-vpc-dstaddr-list))
      ((equal "lvdp" verb) (get-vpc-srcport-list))
      ((equal "lvi" verb) (get-vpc-interface-id-list))
      ((equal "lvip" verb) (get-by-ip (cadr rest)))
      ((equal "lvip2" verb) (get-by-ip2 (cadr rest)))
      ((equal "lvp" verb) (get-vpc-protocols-list))
      ((equal "lvpc" verb) (list-all-vpc))
      ((equal "lvs" verb) (get-vpc-status-list))
      ((equal "lvsa" verb) (get-vpc-srcaddr-list))
      ((equal "lvsp" verb) (get-vpc-srcport-list))
      ((equal "s" verb) (cloudtrail-report-sync (cadr rest)))
      ((equal "sa" verb) (find-by-srcaddr (cadr rest)))
      ((equal "sd" verb) (get-by-date (cadr rest)))
      ((equal "sec" verb) (db-get-by-errorcode (cadr rest)))
      ((equal "seca" verb) (ct-get-all-errors))
      ((equal "sem" verb) (ct-get-by-errormessage (cadr rest)))
      ((equal "ses" verb) (ct-get-by-event-source (cadr rest)))
      ((equal "sev" verb) (db-get-by-event (cadr rest)))
      ((equal "sin" verb) (get-useridentity-by-name (cadr rest)))
      ((equal "sip" verb) (ct-get-by-sourceipaddress (cadr rest)))
      ((equal "sn" verb) (db-get-by-name (cadr rest)))
      ((equal "sp" verb) (find-by-srcport (cadr rest)))
      ((equal "sr" verb) (ct-get-by-region (cadr rest)))
      ((equal "st" verb) (db-get-stats))
      ((equal "uq" verb) (ssdb/unique-queue (cadr rest)))
      ((equal "va" verb) (vpc-flows-report-async (cadr rest) rest))
      ((equal "vda" verb) (vpc-search-by-dstaddr (cadr rest)))
      ((equal "vdp" verb) (vpc-search-by-dstport (cadr rest)))
      ((equal "vs" verb) (vpc-flows-report-sync (cadr rest)))
      ((equal "vsa" verb) (vpc-search-by-srcaddr (cadr rest)))
      ((equal "vsp" verb) (vpc-search-by-srcport (cadr rest)))
((equal "a" verb) (cloudtrail-report-async (cadr rest) (caddr rest)))((equal "ctf" verb) (get-ct-files))
      (t (progn
           (usage))))))

(defun main (&optional argz)
  (let ((args (or argz (argv))))
    #+allegro (setf excl:*tenured-bytes-limit* 5242880000)
    (db-init)
    #+sbcl
    (handler-case (process-args (list "metis" args))
      (sb-sys:interactive-interrupt ()
        (db-close)
        (uiop:quit)))
    #-sbcl
    (progn (process-args (argv))
           (db-close))))

#+allegro
(in-package :cl-user)

#+allegro
(defun main (&rest args)
  (metis::process-args args))
