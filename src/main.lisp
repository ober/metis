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
      ((equal "a" verb) (cloudtrail-report-async (cadr rest) (caddr rest)))
      ((equal "bench" verb) (run-bench))
      ((equal "lc" verb) (get-unique-conversation))
      ((equal "lcts" verb) (get-cts))
      ((equal "lec" verb) (get-errorcode-list))
      ((equal "lev" verb) (get-event-list))
      ((equal "lr" verb) (get-region-list))
      ((equal "lip" verb) (get-sourceips-list))
      ((equal "lsed" verb) (get-serviceEventDetails))
      ((equal "lecat" verb) (get-eventCategory))
      ((equal "ltls" verb) (get-tlsDetails))
      ((equal "lapi" verb) (get-apiVersion))
      ((equal "lconsole" verb) (get-sessionCredentialFromConsole))
      ((equal "lvcep" verb) (get-vpcEndpointId))
      ((equal "ln" verb) (get-name-list))
      ((equal "lva" verb) (get-vpc-action-list))
      ((equal "lvd" verb) (get-vpc-date-list))
      ((equal "lvda" verb) (get-vpc-dstaddr-list))
      ((equal "lvdp" verb) (get-vpc-srcport-list))
      ((equal "lvi" verb) (get-vpc-interface-id-list))
      ((equal "lvip" verb) (get-by-ip (cadr rest)))
      ((equal "lvip2" verb) (get-by-ip2 (cadr rest)))
      ((equal "lvp" verb) (get-vpc-protocols-list))
      ((equal "lvpc" verb) (list-all-vpc))
      ((equal "gre" verb) (get-response-elements))
      ((equal "lvs" verb) (get-vpc-status-list))
      ((equal "lvsa" verb) (get-vpc-srcaddr-list))
      ((equal "lvsp" verb) (get-vpc-srcport-list))
      ((equal "s" verb) (cloudtrail-report-sync (cadr rest)))
      ((equal "sa" verb) (find-by-srcaddr (cadr rest)))
      ((equal "sd" verb) (get-by-date (cadr rest)))
      ((equal "sec" verb) (ct-get-by-errorcode (cadr rest)))
      ((equal "seca" verb) (ct-get-all-errors))
      ((equal "sem" verb) (ct-get-by-errormessage (cadr rest)))
      ((equal "ses" verb) (ct-get-by-event-source (cadr rest)))
      ((equal "sev" verb) (ct-get-by-eventName (cadr rest)))
      ((equal "sin" verb) (get-useridentity-by-name (cadr rest)))
      ((equal "sip" verb) (ct-get-by-sourceipaddress (cadr rest)))
      ((equal "sn" verb) (ct-get-by-name (cadr rest)))
      ((equal "sr" verb) (ct-get-by-region (cadr rest)))
      ((equal "sp" verb) (find-by-srcport (cadr rest)))
      ((equal "st" verb) (get-stats))
      ((equal "va" verb) (vpc-flows-report-async (cadr rest) rest))
      ((equal "vda" verb) (vpc-search-by-dstaddr (cadr rest)))
      ((equal "vdp" verb) (vpc-search-by-dstport (cadr rest)))
      ((equal "vs" verb) (vpc-flows-report-sync (cadr rest)))
      ((equal "vsa" verb) (vpc-search-by-srcaddr (cadr rest)))
      ((equal "vsp" verb) (vpc-search-by-srcport (cadr rest)))
      ((equal "ls" verb) (sync-from-s3 (cadr rest)))
      ((equal "ctf" verb) (get-ct-files))
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
