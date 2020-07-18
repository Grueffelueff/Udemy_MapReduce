from mrjob.job import MRJob

class MRMin_by_Location(MRJob):
    def mapper(self, key, line):
        (station_ID, date, minmax, value,x,y,z,w) = line.split(',')
        if (minmax == "TMIN"):
            yield station_ID, value

    def reducer(self, station_ID, value):
        yield station_ID, min(value)
        
class MRMax_by_Location(MRJob):
    def mapper(self, key, line):
        (station_ID, date, minmax, value,x,y,z,w) = line.split(',')
        if (minmax == "TMAX"):
            yield station_ID, value

    def reducer(self, station_ID, value):
        yield station_ID, max(value)

if __name__ == '__main__':
    #MRMin_by_Location.run()
    MRMax_by_Location.run()
