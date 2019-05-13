parse_yaml() {
    local yaml_file=$1
    local prefix=$2
    local s
    local w
    local fs

    s='[[:space:]]*'
    w='[a-zA-Z0-9_.-]*'
    fs="$(echo @|tr @ '\034')"

    (
        sed -e '/- [^\“]'"[^\']"'.*: /s|\([ ]*\)- \([[:space:]]*\)|\1-\'$'\n''  \1\2|g' |

        sed -ne '/^--/s|--||g; s|\"|\\\"|g; s/[[:space:]]*$//g;' \
            -e "/#.*[\"\']/!s| #.*||g; /^#/s|#.*||g;" \
            -e "s|^\($s\)\($w\)$s:$s\"\(.*\)\"$s\$|\1$fs\2$fs\3|p" \
            -e "s|^\($s\)\($w\)${s}[:-]$s\(.*\)$s\$|\1$fs\2$fs\3|p" |

        awk -F"$fs" '{
            indent = length($1)/2;
            if (length($2) == 0) { conj[indent]="+";} else {conj[indent]="";}
            vname[indent] = $2;
            for (i in vname) {if (i > indent) {delete vname[i]}}
                if (length($3) > 0) {
                    vn=""; for (i=0; i<indent; i++) {vn=(vn)(vname[i])("_")}
                    printf("%s%s%s%s=(\"%s\")\n", "'"$prefix"'",vn, $2, conj[indent-1],$3);
                }
            }' |

        sed -e 's/_=/+=/g' |

        awk 'BEGIN {
                FS="=";
                OFS="="
            }
            /(-|\.).*=/ {
                gsub("-|\\.", "_", $1)
            }
            { print }'
    ) < "$yaml_file"
}

ALL=empty
CLIENT=empty
CLUSTER=empty
NODE=empty

while getopts "n:ahc:o:" opt; do
  case ${opt} in
    c ) CLUSTER=${OPTARG}
      ;;
    o ) NODE=${OPTARG}
      ;;
    n ) CLIENT=${OPTARG}
      ;;
    a ) ALL=true
      ;;
    h ) echo "-n <name>    run bob with <name> from cluster config"
        echo "-a                run all bobs from cluster config"
        echo "-c                cluster config path"
        echo "-o                node config path"
        echo "-h                help"
      ;;
    \? ) echo "Usage: type [-h]"
      ;;
  esac
done

if [ "$CLUSTER" = empty ] ; then
    echo "need cluster config argument. Type '-h' for information"
    exit 1
fi
if [ "$NODE" = empty ] ; then
    echo "need node config argument. Type '-h' for information"
    exit 1
fi
if [ "$ALL" = empty ] && [ "$CLIENT" = empty ] ; then
    echo "need 'n' or 'a' argument. Type '-h' for information"
    exit 1
fi
if [ "$ALL" = true ] && [ "$CLIENT" != empty ] ; then
    echo "can't use argument 'c' and 'a' together"
    exit 1
fi

eval "$(parse_yaml "$CLUSTER" "cluster_")"

if [ "$ALL" = true ] 
then
  i=0
  bob_cmd="tmux new-session "
  while [ "${cluster_nodes___name[$i]}" ] ; do
      echo "run bob:" "${cluster_nodes___name[$i]}"
      if [ "$i" != 0 ] ; then
        bob_cmd+=" \; split-window "
      fi
      bob_cmd+=" \"./target/debug/bobd -c $CLUSTER -n $NODE -a ${cluster_nodes___name[$i]}; read -p 'Press enter to continue' \" \; select-layout tiled"
      ((i++))
  done
  eval $bob_cmd
else ./target/debug/bobd -c $CLUSTER -n $NODE -a $CLIENT
fi