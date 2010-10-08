
INPUT = 'TXT-PARA-SUGAR.txt'
OUTPUT = 'grupo-orden-marca.txt'

def main():

    input = open(INPUT, 'r')
    output = open (OUTPUT, 'w')
    
    for line in input.readlines():
        data = line.strip().split(';')
        
        if (data[1] != data[5]) or (data[2] != data[6]):
            # Grupo y orden no coinciden
            print " %s != %s : %s"%(data[1],data[5],data[1] != data[5])
            print " %s != %s : %s"%(data[2],data[6],data[2] != data[6])
            
            continue
        
        output.write(line)
        
if __name__=="__main__":
    main()
    
    