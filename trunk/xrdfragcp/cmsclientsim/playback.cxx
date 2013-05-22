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
const char *postfix = "?tried=xrootd.t2.ucsd.edu";


int main()
{
  gSystem->Load("libSXrdClasses");

  // Chain up several reports
  TChain mychain("XrdFar");

  mychain.Add("/net/xrootd.t2/data/xrdmon/far/xmfar-2013-03-03-*.root");

  //mychain.Add("/net/xrootd.t2/data/xrdmon/far/xmfar-2013-03-*.root");
  //mychain.Add("/net/xrootd.t2/data/xrdmon/far/xmfar-2013-04-*.root");
  //mychain.Add("/net/xrootd.t2/data/xrdmon/far/xmfar-2013-05-*.root");

  // Get set up to read domain data from the chain
  SXrdFileInfo   F, *fp = &F;
  SXrdUserInfo   U, *up = &U;
  SXrdServerInfo S, *sp = &S;

  mychain.SetBranchAddress("F.", &fp);
  mychain.SetBranchAddress("U.", &up);
  mychain.SetBranchAddress("S.", &sp);

  Long64_t N = mychain.GetEntries();
  printf("Total of %lld entries found.\n", N);

  std::multimap<Long64_t, Long64_t> time_to_entry;


  // Select entries and collect open times.

  for (Long64_t i = 0; i < N; ++i)
  {
    mychain.GetEntry(i);

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
    mychain.GetEntry(i->second);

    if (i->first > prev_time && prev_time != 0)
    {
      printf("sleep %lld \n", i->first - prev_time);
    }
    prev_time = i->first;

    printf("xrdfragcp --cmsclientsim %lld %d %lld %s%s%s\n",
           TMath::Nint(1024*1024*F.mReadStats.mSumX),
           F.mReadStats.mN,
           F.mCloseTime - F.mOpenTime,
           prefix, F.mName.Data(), postfix);
  }

  return 0;
}
