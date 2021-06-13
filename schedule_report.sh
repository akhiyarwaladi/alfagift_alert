#!/bin/sh
# >>> conda initialize >>>
# !! Contents within this block are managed by 'conda init' !!
__conda_setup="$('/opt/anaconda3/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
if [ $? -eq 0 ]; then
    eval "$__conda_setup"
else
    if [ -f "/opt/anaconda3/etc/profile.d/conda.sh" ]; then
        . "/opt/anaconda3/etc/profile.d/conda.sh"
    else
        export PATH="/opt/anaconda3/bin:$PATH"
    fi
fi
unset __conda_setup
# <<< conda initialize <<<


export PATH="$PATH:/opt/oracle/instantclient_19_8"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/opt/oracle/instantclient_19_8"

jupyter-book build /home/server/gli-data-science/akhiyar/alfagift_alert/exploration.ipynb --builder pdfhtml

rm -rf /var/www/html/gli_data_science/_build/
mv /home/server/gli-data-science/akhiyar/alfagift_alert/_build /var/www/html/gli_data_science
