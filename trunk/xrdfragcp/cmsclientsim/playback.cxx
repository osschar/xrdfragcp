//Quick script to get the names of all the domains used during a particular
//timeframe, in order to set up smarter domain name parsing

#include "SXrdClasses.h"

#include "TTree.h"
#include "TBranch.h"
#include "TBranchElement.h"
#include "TChain.h"
#include "TSystem.h"

#include <map>


const char *prefix  = "root://xrootd-proxy.t2.ucsd.edu/";

// const char *postfix = "?tried=xrootd.t2.ucsd.edu";
const char *postfix = " > /dev/null 2>&1 &";


int main()
{
  // Chain up several reports
  TChain chain("XrdFar");

  chain.Add("/net/xrootd.t2/data/xrdmon/far/xmfar-2013-03-03-*.root");
  chain.Add("/net/xrootd.t2/data/xrdmon/far/xmfar-2013-03-04-*.root");
  chain.Add("/net/xrootd.t2/data/xrdmon/far/xmfar-2013-03-05-*.root");

  // chain.ls();

  // Get set up to read domain data from the chain
  SXrdFileInfo   F, *fp = &F;
  SXrdUserInfo   U, *up = &U;
  SXrdServerInfo S, *sp = &S;

  chain.SetBranchAddress("F.", &fp);
  chain.SetBranchAddress("U.", &up);
  chain.SetBranchAddress("S.", &sp);

  Long64_t N = chain.GetEntries();
  printf("# Total of %lld entries found.\n", N);


  chain.GetEntry(0);
  Long64_t first_close_time = F.mCloseTime;


  std::multimap<Long64_t, Long64_t> time_to_entry;


  // Select entries and collect open times.

  for (Long64_t i = 0; i < N; ++i)
  {
    chain.GetEntry(i);

    // Select only files opened after the close-time of first entry.
    // This prevents a slow raise-up in file-open rate.
    if (F.mOpenTime < first_close_time)
      continue;

    if ( ! U.mFromDomain.EndsWith("t2.ucsd.edu"))
      continue;

    if ( F.mName.BeginsWith("/store/test"))
      continue;

    time_to_entry.insert(std::make_pair(F.mOpenTime, i));
  }


  // Loop over files as they were being opened

  Long64_t prev_time = 0;

  for (auto i = time_to_entry.begin(); i != time_to_entry.end(); ++i)
  {
    chain.GetEntry(i->second);

    if (i->first > prev_time && prev_time != 0)
    {
      Long64_t dt = i->first - prev_time;

      // HACK ... do not wait more than 100 seconds.
      // This was needed due to slow ramp-up of file opens as entries get
      // written out at close-time.
      // if (dt > 100)
      //   dt = 100;

      printf("echo sleep %lld\n", dt);
      printf("sleep %lld\n", dt);
    }
    prev_time = i->first;

    printf("echo Opening %s\n", F.mName.Data());

    printf("xrdfragcp --cmsclientsim %lld %d %lld %s%s%s\n",
           TMath::Nint(1024*1024*F.mReadStats.mSumX),
           F.mReadStats.mN,
           F.mCloseTime - F.mOpenTime,
           prefix, F.mName.Data(), postfix);
  }

  return 0;
}
